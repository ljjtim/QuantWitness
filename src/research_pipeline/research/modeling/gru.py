"""Qlib 序列模型 的完整窗口绑定、CPU 执行和可迁移文件状态。"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import importlib.metadata
import importlib

import numpy as np
import pandas as pd

from .inputs import QlibModelError, qlib_matrix
from .sequence import SequenceWindows, qlib_sequence_dataset


SEQUENCE_BUNDLE_SCHEMA = "research.qlib-sequence-model-bundle.v1"
SEQUENCE_MODEL_MODULES = {
    "GRU": ("qlib.contrib.model.pytorch_gru_ts", "GRU", "GRU_model"),
    "LSTM": ("qlib.contrib.model.pytorch_lstm_ts", "LSTM", "LSTM_model"),
    "TransformerModel": ("qlib.contrib.model.pytorch_transformer_ts", "TransformerModel", "model"),
}


def require_sequence_environment():
    for distribution, expected in (("pyqlib", "0.9.7"), ("torch", "2.5.1")):
        try:
            actual = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError as exc:
            raise QlibModelError(f"序列模型依赖缺失: {distribution}") from exc
        if actual.split("+", 1)[0] != expected:
            raise QlibModelError(f"序列模型要求 {distribution}=={expected}，当前为 {actual}")


def model_network(model, model_class):
    """返回固定 Qlib TS 模型的网络对象，统一序列文件合同。"""
    try:
        module_path, class_name, attribute = SEQUENCE_MODEL_MODULES[model_class]
    except KeyError as exc:
        raise QlibModelError(f"不支持的序列模型: {model_class}") from exc
    if type(model) is not getattr(importlib.import_module(module_path), class_name):
        raise QlibModelError("恢复的序列模型类型与候选声明不一致")
    network = getattr(model, attribute, None)
    if network is None:
        raise QlibModelError("序列模型缺少网络状态")
    return network


def preserve_batch_axis(module, inputs, output):
    """单样本仍返回长度为一的预测向量，供上游训练掩码和拼接使用。"""
    return output.reshape(-1)


@contextmanager
def cpu_execution():
    import torch

    threads = torch.get_num_threads()
    numpy_state, torch_state = np.random.get_state(), torch.get_rng_state()
    try:
        torch.set_num_threads(1)
        yield
    finally:
        np.random.set_state(numpy_state)
        torch.set_rng_state(torch_state)
        torch.set_num_threads(threads)


def effective_kwargs(candidate, feature_columns, root_seed):
    kwargs = dict(candidate["model"]["kwargs"])
    if kwargs.get("d_feat", len(feature_columns)) != len(feature_columns):
        raise QlibModelError("序列模型 d_feat 必须等于每个时点的特征数")
    kwargs.update(d_feat=len(feature_columns), batch_size=1, GPU=-1, n_jobs=0, seed=root_seed)
    return kwargs


def bind_windows(windows, frame, feature_columns, step_len):
    """绑定实际末端及历史成员，只保留本次拟合或预测需要的无标签上下文。"""
    if not isinstance(windows, SequenceWindows):
        raise QlibModelError("序列模型 必须提供 sequence_context=SequenceWindows")
    if windows.step_len != step_len or windows.feature_columns != tuple(feature_columns):
        raise QlibModelError("序列模型 窗口长度或特征顺序与冻结声明不一致")
    ids = frame["sample_id"].astype(str).tolist()
    if not ids or len(ids) != len(set(ids)):
        raise QlibModelError("序列模型 末端必须非空且身份唯一")
    endpoints = windows.targets.set_index("sample_id")
    if endpoints.index.has_duplicates or not set(ids) <= set(endpoints.index):
        raise QlibModelError("序列模型 上下文没有唯一覆盖全部末端")
    if set(ids) & set(windows.exclusions["sample_id"]):
        raise QlibModelError("序列模型 末端存在未满足完整窗口的样本")
    days = tuple(pd.Timestamp(day).date() for day in windows.calendar_sessions)
    if not days or tuple(sorted(set(days))) != days:
        raise QlibModelError("序列模型 冻结日历无效")
    positions = {day: i for i, day in enumerate(days)}
    context = windows.context.copy()
    context["observation_session"] = context["observation_session"].map(lambda value: pd.Timestamp(value).date())
    if context.duplicated(["observation_session", "entity_id"]).any():
        raise QlibModelError("序列模型 上下文证券会话重复")
    source = context.set_index(["observation_session", "entity_id"])
    members = windows.members.loc[windows.members.sample_id.isin(ids)].copy()
    if members.duplicated(["sample_id", "step"]).any() or len(members) != len(ids) * step_len:
        raise QlibModelError("序列模型 窗口成员数量或步序无效")
    groups = {sid: group.sort_values("step") for sid, group in members.groupby("sample_id")}
    used_keys = set()
    for target in frame.to_dict("records"):
        sid, entity = str(target["sample_id"]), str(target["entity_id"])
        day, decision = pd.Timestamp(target["observation_session"]).date(), pd.Timestamp(target["decision_time"])
        bound = endpoints.loc[sid]
        if (str(bound.entity_id) != entity or pd.Timestamp(bound.observation_session).date() != day
                or pd.Timestamp(bound.decision_time) != decision or pd.isna(decision)):
            raise QlibModelError("序列模型 实际末端与窗口决策事实不一致")
        end = positions.get(day, -1)
        if end < step_len - 1:
            raise QlibModelError("序列模型 窗口历史不足")
        group = groups.get(sid)
        if group is None:
            raise QlibModelError("序列模型 末端缺少窗口成员")
        if group.step.tolist() != list(range(step_len)):
            raise QlibModelError("序列模型 窗口步序不连续")
        for member, member_day in zip(group.to_dict("records"), days[end - step_len + 1:end + 1]):
            key = (member_day, entity)
            if key not in source.index or (member["entity_id"], pd.Timestamp(member["observation_session"]).date()) != (entity, member_day):
                raise QlibModelError("序列模型 窗口会话或证券不一致")
            row = source.loc[key]
            observed, available = pd.Timestamp(row.observation_time), pd.Timestamp(row.feature_available_time)
            if (row.feature_count != len(feature_columns) or pd.isna(observed) or pd.isna(available)
                    or observed > available or available > decision):
                raise QlibModelError("序列模型 历史特征不完整或在末端决策时点尚不可见")
            if (pd.Timestamp(member["observation_time"]) != observed
                    or pd.Timestamp(member["feature_available_time"]) != available
                    or member["feature_lineage_hash"] != row.feature_lineage_hash):
                raise QlibModelError("序列模型 窗口成员与特征上下文不一致")
            used_keys.add(key)
        current = source.loc[(day, entity)]
        if not np.array_equal(np.asarray([target[name] for name in feature_columns], dtype=float),
                              current[list(feature_columns)].to_numpy(dtype=float), equal_nan=True):
            raise QlibModelError("序列模型 末端特征值与上下文不一致")
    selected_context = source.loc[sorted(used_keys)].reset_index()
    return replace(windows, context=selected_context, targets=endpoints.loc[ids].reset_index(),
                   members=members.reset_index(drop=True), exclusions=windows.exclusions.iloc[:0].copy())


def processed_dataset(windows, *, processors, segments, label_frame=None):
    """处理器已由合格 train 末端拟合；历史特征不受 DropnaLabel 删除影响。"""
    columns = windows.feature_columns
    source = windows.context.set_index(["observation_session", "entity_id"])
    index = pd.MultiIndex.from_arrays([pd.to_datetime(source.index.get_level_values(0)),
                                       source.index.get_level_values(1)], names=["datetime", "instrument"])
    data = pd.DataFrame(source[list(columns)].to_numpy(float), index=index,
                        columns=pd.MultiIndex.from_product([["feature"], columns]))
    for processor in processors["infer"]:
        data = processor(data.copy())
    if not data.index.equals(index) or list(data["feature"].columns) != list(columns):
        raise QlibModelError("序列模型 特征处理器改变了历史索引或特征顺序")
    values = data["feature"].to_numpy(dtype=float)
    if not np.isfinite(values).all() or np.any(np.abs(values) > np.finfo(np.float32).max):
        raise QlibModelError("序列模型 处理后的历史特征必须为有限 float32 数值")
    transformed = pd.DataFrame(values, index=source.index, columns=columns)
    labels = None
    if label_frame is not None:
        learned = qlib_matrix(label_frame, columns, label=True)
        original_index = learned.index
        for processor in processors["learn"]:
            learned = processor(learned.copy())
        if not learned.index.equals(original_index):
            raise QlibModelError("序列模型 标签处理器不得删除合格末端")
        wanted = pd.MultiIndex.from_arrays([pd.to_datetime(label_frame.observation_session).dt.tz_localize(None),
                                            label_frame.entity_id.astype(str)], names=["datetime", "instrument"])
        target = learned[("label", "target")].reindex(wanted).to_numpy(float)
        if not np.isfinite(target).all() or np.any(np.abs(target) > np.finfo(np.float32).max):
            raise QlibModelError("序列模型 训练标签必须为有限 float32 数值")
        labels = pd.Series(target, index=label_frame.sample_id.astype(str))
    return qlib_sequence_dataset(windows, segments=segments, labels=labels, transformed_features=transformed)


def save_window_facts(root, bundle_path, windows):
    paths = {}
    for name in ("context", "targets", "members"):
        relative = f"{bundle_path}/sequence/{name}.parquet"
        path = Path(root) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        getattr(windows, name).to_parquet(path, index=False)
        paths[name] = relative
    return {"schema": "research.qlib-sequence-input.v1", "files": paths,
            "step_len": windows.step_len, "feature_columns": list(windows.feature_columns),
            "calendar_sessions": [str(day) for day in windows.calendar_sessions],
            "missing_policy": "complete_window", "batch_axis": "vector"}


def verify_loaded_model(root, config, model):
    import torch
    model_class = config["candidate"]["model"]["class"]
    network = model_network(model, model_class)
    if not model.fitted or str(model.device) != "cpu":
        raise QlibModelError("恢复的序列模型类型、拟合状态或设备不符合声明")
    kwargs = effective_kwargs(config["candidate"], config["feature_columns"], config["root_seed"])
    if config["effective_model_kwargs"] != kwargs:
        raise QlibModelError("恢复的序列模型实际参数与候选声明不一致")
    defaults = {"hidden_size":64, "num_layers":2, "dropout":0.0, "n_epochs":200,
                "lr":0.001, "metric":"", "early_stop":20, "loss":"mse", "optimizer":"adam"}
    names = ("d_feat", "hidden_size", "num_layers", "dropout", "n_epochs", "metric", "batch_size", "early_stop", "loss", "optimizer", "n_jobs", "seed")
    if model_class == "TransformerModel":
        defaults.update(d_model=64, n_epochs=100, lr=0.0001, early_stop=5, reg=1e-3)
        names = ("d_model", "dropout", "n_epochs", "lr", "reg", "metric", "batch_size", "early_stop", "loss", "optimizer", "n_jobs", "seed")
    for name in names:
        expected = kwargs.get(name, defaults.get(name))
        if name == "optimizer": expected = expected.lower()
        if getattr(model, name) != expected:
            raise QlibModelError(f"恢复的序列模型 {name} 与封存参数不一致")
    weights = torch.load(Path(root) / config["weights_path"], map_location="cpu", weights_only=True)
    actual = network.state_dict()
    if (set(weights) != set(actual) or any(not torch.equal(weights[name], actual[name]) for name in weights)
            or any(not torch.isfinite(value).all().item() for value in actual.values())):
        raise QlibModelError("序列模型显式权重与模型文件不一致或含非有限值")
    if model_class == "TransformerModel":
        verify_transformer_network(model, network, kwargs)
    elif (network.rnn.input_size != kwargs["d_feat"] or network.rnn.hidden_size != kwargs.get("hidden_size", 64)
            or network.rnn.num_layers != kwargs.get("num_layers", 2)
            or network.rnn.dropout != kwargs.get("dropout", 0.0)
            or not network.rnn.batch_first or network.fc_out.out_features != 1
            or any(group["lr"] != kwargs.get("lr", 0.001) for group in model.train_optimizer.param_groups)):
        raise QlibModelError("序列模型网络结构或学习率与封存参数不一致")
    if preserve_batch_axis not in network._forward_hooks.values():
        raise QlibModelError("序列模型缺少单样本输出向量合同")


def verify_transformer_network(model, network, kwargs):
    """核对固定TS Transformer的投影、注意力、位置编码及优化器。"""
    import torch
    width, heads = kwargs.get("d_model", 64), kwargs.get("nhead", 2)
    layers, dropout = kwargs.get("num_layers", 2), kwargs.get("dropout", 0.0)
    if (network.d_feat != kwargs["d_feat"] or network.feature_layer.in_features != kwargs["d_feat"]
            or network.feature_layer.out_features != width or network.decoder_layer.in_features != width
            or network.decoder_layer.out_features != 1 or tuple(network.pos_encoder.pe.shape) != (1000, 1, width)
            or len(network.transformer_encoder.layers) != layers):
        raise QlibModelError("TransformerModel投影、位置编码或层数与封存参数不一致")
    for layer in [network.encoder_layer, *network.transformer_encoder.layers]:
        if (layer.self_attn.embed_dim != width or layer.self_attn.num_heads != heads
                or layer.self_attn.batch_first or layer.self_attn.dropout != dropout
                or layer.linear1.in_features != width or layer.linear1.out_features != 2048
                or layer.linear2.in_features != 2048 or layer.linear2.out_features != width
                or any(part.p != dropout for part in (layer.dropout, layer.dropout1, layer.dropout2))):
            raise QlibModelError("TransformerModel注意力或前馈结构与封存参数不一致")
    optimizer_class = torch.optim.Adam if kwargs.get("optimizer", "adam").lower() == "adam" else torch.optim.SGD
    if (type(model.train_optimizer) is not optimizer_class
            or any(group["lr"] != kwargs.get("lr", 0.0001)
                   or group["weight_decay"] != kwargs.get("reg", 1e-3)
                   for group in model.train_optimizer.param_groups)):
        raise QlibModelError("TransformerModel优化器、学习率或正则化与封存参数不一致")
