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


SEQUENCE_BUNDLE_SCHEMA = "research.qlib-sequence-model-bundle.v2"
SEQUENCE_MODEL_MODULES = {
    "GRU": ("qlib.contrib.model.pytorch_gru_ts", "GRU", "GRU_model"),
    "LSTM": ("qlib.contrib.model.pytorch_lstm_ts", "LSTM", "LSTM_model"),
    "TransformerModel": ("qlib.contrib.model.pytorch_transformer_ts", "TransformerModel", "model"),
    "GATs": ("qlib.contrib.model.pytorch_gats_ts", "GATs", "GAT_model"),
}


def require_sequence_environment():
    for distribution, expected in (("pyqlib", {"0.9.7"}), ("torch", {"2.9.1"})):
        try:
            actual = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError as exc:
            raise QlibModelError(f"序列模型依赖缺失: {distribution}") from exc
        if actual.split("+", 1)[0] not in expected:
            raise QlibModelError(f"序列模型要求 {distribution}=={expected}，当前为 {actual}")


def model_network(model, model_class):
    """返回固定 Qlib TS 模型的网络对象，统一序列文件合同。"""
    if model_class == "ProjectQlibModel":
        attribute = getattr(model, "_rp_network_attribute", None)
        network = getattr(model, attribute, None) if isinstance(attribute, str) else None
        if network is None:
            raise QlibModelError("项目模型缺少冻结网络属性")
        return network
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
def cpu_execution(GPU=-1):
    import torch

    threads = torch.get_num_threads()
    numpy_state, torch_state = np.random.get_state(), torch.get_rng_state()
    cuda_state = None
    if GPU >= 0:
        if not torch.cuda.is_available() or GPU >= torch.cuda.device_count():
            raise QlibModelError("声明的CUDA设备不可用")
        cuda_state = torch.cuda.get_rng_state_all()
        if torch.__version__ != "2.9.1+cu130":
            raise QlibModelError("CUDA序列模型要求torch==2.9.1+cu130")
        torch.cuda.reset_peak_memory_stats(GPU)
    try:
        torch.set_num_threads(1)
        yield
    finally:
        np.random.set_state(numpy_state)
        torch.set_rng_state(torch_state)
        if cuda_state is not None:
            torch.cuda.set_rng_state_all(cuda_state)
        torch.set_num_threads(threads)


def batch_contract(candidate):
    model = candidate["model"]
    default = "daily_cross_section" if model["class"] == "GATs" else "sample"
    if model["class"] == "ProjectQlibModel":
        default = model["kwargs"]["adapter"]["batch_contract"]
    actual = candidate["dataset"].get("batch_contract", default)
    if model["class"] == "ProjectQlibModel" and actual != default:
        raise QlibModelError("项目模型批次合同与adapter不一致")
    return actual


def effective_kwargs(candidate, feature_columns, root_seed):
    kwargs = dict(candidate["model"]["kwargs"])
    if kwargs.get("d_feat", len(feature_columns)) != len(feature_columns):
        raise QlibModelError("序列模型 d_feat 必须等于每个时点的特征数")
    kwargs.update(d_feat=len(feature_columns), GPU=kwargs.get("GPU", -1), n_jobs=0, seed=root_seed)
    if batch_contract(candidate) == "sample":
        kwargs.setdefault("batch_size", 1)
    return kwargs


def daily_prediction_frame(windows, frame):
    """图节点只由当日完整Feature窗口决定，不由未来标签是否存在决定。"""
    days = set(pd.to_datetime(frame.observation_session).dt.date)
    targets = windows.targets.loc[
        windows.targets.observation_session.isin(days)
        & ~windows.targets.sample_id.isin(windows.exclusions.sample_id)].copy()
    context = windows.context.loc[windows.context.observation_session.isin(days)]
    expanded = targets.merge(context, on=["entity_id", "observation_session"], how="left", validate="one_to_one")
    if not set(frame.sample_id.astype(str)) <= set(expanded.sample_id.astype(str)):
        raise QlibModelError("每日图预测请求不在完整Feature末端集合中")
    return expanded.sort_values(["observation_session", "entity_id"]).reset_index(drop=True)


class _DailyBatch:
    """只去掉DataLoader外层轴，保留单证券日的股票轴。"""
    def __init__(self, value):
        self.value = value

    def squeeze(self):
        return self.value.squeeze(0)


def _vector_sample_values(samples, indexes):
    """对原生采样器现有索引按当前批次取窗，不预展开全历史。"""
    indexes = np.asarray(indexes, dtype=np.int64)
    if indexes.ndim != 1 or ((indexes < 0) | (indexes >= len(samples))).any():
        raise KeyError("序列批次索引超出范围")
    if not len(indexes):
        return np.empty((0, samples.step_len, *samples.data_arr.shape[1:]), dtype=samples.data_arr.dtype)
    coordinates = samples.idx_map[indexes]
    rows = coordinates[:, 0, None] - np.arange(samples.step_len - 1, -1, -1)
    columns = coordinates[:, 1, None]
    values = samples.idx_arr[np.maximum(rows, 0), columns].copy()
    values[rows < 0] = np.nan
    valid = ~np.isnan(values)
    steps = np.arange(samples.step_len)[None, :]
    if samples.fillna_type in {"ffill", "ffill+bfill"}:
        previous = np.maximum.accumulate(np.where(valid, steps, -1), axis=1)
        filled = np.take_along_axis(values, np.maximum(previous, 0), axis=1)
        filled[previous < 0] = np.nan
        values = filled
        if samples.fillna_type == "ffill+bfill":
            valid = ~np.isnan(values)
            following = np.minimum.accumulate(np.where(valid, steps, samples.step_len)[:, ::-1], axis=1)[:, ::-1]
            values = np.take_along_axis(values, np.minimum(following, samples.step_len - 1), axis=1)
    elif samples.fillna_type != "none":
        raise QlibModelError("不支持的原生采样填补政策")
    positions = np.nan_to_num(values.astype(np.float64), nan=samples.nan_idx).astype(int)
    return samples.data_arr[positions]


class _BatchSamples:
    """样本批次沿用原顺序，PyTorch批量接口一次读取当前batch。"""
    def __init__(self, samples, *, daily=False):
        self.samples = samples
        self.index = samples.get_index().sort_values() if daily else samples.get_index()
        self.order = samples.get_index().get_indexer(self.index)

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, index):
        if isinstance(index, tuple):
            return self.samples[index]
        if isinstance(index, (int, np.integer)):
            if not 0 <= index < len(self):
                raise KeyError("序列样本索引超出范围")
            return _vector_sample_values(self.samples, self.order[[index]])[0]
        indexes = np.asarray(index, dtype=np.int64)
        if indexes.ndim != 1 or ((indexes < 0) | (indexes >= len(self))).any():
            raise KeyError("序列批次索引超出范围")
        return _vector_sample_values(self.samples, self.order[indexes])

    def __getitems__(self, indexes):
        import torch
        return torch.as_tensor(self[indexes])

    def __getattr__(self, name):
        return getattr(self.samples, name)

    def get_index(self):
        return self.index


class _DailySamples(_BatchSamples):
    """每日图索引按日期、证券排列，取值仍映射原生采样器。"""
    def __init__(self, samples):
        super().__init__(samples, daily=True)

    def __getitems__(self, indexes):
        return [self[index] for index in indexes]


class _DailyDataset:
    def __init__(self, dataset, *, daily=True):
        self.dataset = dataset
        self.daily = daily

    def prepare(self, segment, **kwargs):
        result = self.dataset.prepare(segment, **kwargs)
        return _DailySamples(result) if self.daily else _BatchSamples(result)


class _BatchConcat:
    """保留Qlib样本与权重配对，按本批索引交给原生张量转换。"""
    def __init__(self, dataset):
        self.dataset = dataset

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, index):
        return self.dataset[index]

    def __getitems__(self, indexes):
        import torch
        return tuple(torch.as_tensor(part[indexes]) for part in self.dataset.datasets)


def _identity_batch(batch):
    return batch


def model_dataset(dataset, candidate):
    return _DailyDataset(dataset, daily=batch_contract(candidate) == "daily_cross_section")


class _DailyLoader:
    def __init__(self, loader):
        self.loader = loader

    def __iter__(self):
        for value in self.loader:
            yield _DailyBatch(value)


@contextmanager
def upstream_loader_contract(model, candidate):
    """保持上游训练算法，补齐尾批及每日批次的维度合同。"""
    module = importlib.import_module(model.train_epoch.__func__.__module__)
    original = module.DataLoader
    daily = batch_contract(candidate) == "daily_cross_section"

    def loader(*args, **kwargs):
        kwargs["drop_last"] = False
        if not daily:
            dataset = args[0] if args else kwargs["dataset"]
            from qlib.model.utils import ConcatDataset
            if isinstance(dataset, ConcatDataset) and isinstance(dataset.datasets[0], _BatchSamples):
                dataset = _BatchConcat(dataset)
            if isinstance(dataset, (_BatchSamples, _BatchConcat)):
                kwargs["collate_fn"] = _identity_batch
                if args:
                    args = (dataset, *args[1:])
                else:
                    kwargs["dataset"] = dataset
        value = original(*args, **kwargs)
        return _DailyLoader(value) if daily else value

    try:
        module.DataLoader = loader
        yield
    finally:
        module.DataLoader = original


def bind_windows(windows, frame, feature_columns, step_len):
    """按紧凑成员分块核对窗口，绑定本次实际消费的无标签上下文。"""
    if not isinstance(windows, SequenceWindows):
        raise QlibModelError("序列模型 必须提供 sequence_context=SequenceWindows")
    columns = tuple(feature_columns)
    if windows.step_len != step_len or windows.feature_columns != columns:
        raise QlibModelError("序列模型 窗口长度或特征顺序与冻结声明不一致")
    ids = pd.Index(frame["sample_id"].astype(str))
    if ids.empty or ids.has_duplicates:
        raise QlibModelError("序列模型 末端必须非空且身份唯一")
    endpoint_index = pd.Index(windows.targets.sample_id)
    target_positions = endpoint_index.get_indexer(ids) if not endpoint_index.has_duplicates else np.full(len(ids), -1)
    if (target_positions < 0).any():
        raise QlibModelError("序列模型 上下文没有唯一覆盖全部末端")
    if ids.isin(windows.exclusions.sample_id).any():
        raise QlibModelError("序列模型 末端存在未满足完整窗口的样本")
    calendar = pd.DatetimeIndex(windows.calendar_sessions).normalize()
    if calendar.empty or calendar.hasnans or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise QlibModelError("序列模型 冻结日历无效")
    target_days = pd.DatetimeIndex(frame.observation_session).normalize()
    target_entities = frame.entity_id.astype(str).to_numpy()
    decisions = pd.to_datetime(frame.decision_time, utc=True)
    bound = windows.targets.iloc[target_positions]
    if (not np.array_equal(bound.entity_id.astype(str).to_numpy(), target_entities)
            or not pd.DatetimeIndex(bound.observation_session).normalize().equals(target_days)
            or decisions.isna().any()
            or not np.array_equal(pd.to_datetime(bound.decision_time, utc=True).astype("int64"), decisions.astype("int64"))):
        raise QlibModelError("序列模型 实际末端与窗口决策事实不一致")
    target_calendar_positions = calendar.get_indexer(target_days)
    if (target_calendar_positions < step_len - 1).any():
        raise QlibModelError("序列模型 窗口历史不足")
    context = windows.context
    if (set(windows.members.columns) != {"sample_id", "step", "context_index"}
            or "context_index" not in context or context.context_index.duplicated().any()
            or not pd.api.types.is_integer_dtype(context.context_index)
            or not pd.api.types.is_integer_dtype(windows.members.context_index)
            or not pd.api.types.is_integer_dtype(windows.members.step)):
        raise QlibModelError("序列模型上下文索引必须为唯一整数，成员须使用紧凑结构")
    context_days = pd.DatetimeIndex(context.observation_session).normalize()
    context_keys = pd.MultiIndex.from_arrays([context_days, context.entity_id.astype(str)])
    if context_keys.has_duplicates:
        raise QlibModelError("序列模型 上下文证券会话重复")
    selected = windows.members.sample_id.isin(ids)
    members = (windows.members if selected.all()
               else windows.members.loc[selected].reset_index(drop=True))
    if not members.index.equals(pd.RangeIndex(len(members))):
        members = members.copy(deep=False)
        members.index = pd.RangeIndex(len(members))
    member_targets = ids.get_indexer(members.sample_id)
    steps = members.step.to_numpy(dtype=np.int64)
    if len(members) != len(ids) * step_len or (steps < 0).any() or (steps >= step_len).any():
        raise QlibModelError("序列模型 窗口成员数量或步序无效")
    # 每个末端的step恰好覆盖0至step_len-1，避免按样本创建大量小DataFrame。
    linear = member_targets * step_len + steps
    if len(np.unique(linear)) != len(linear):
        raise QlibModelError("序列模型 窗口成员数量或步序无效")
    del linear, selected
    member_context = pd.Index(context.context_index).get_indexer(members.context_index)
    if (member_context < 0).any():
        raise QlibModelError("序列模型成员引用不存在的上下文")
    actual_days = context_days.to_numpy(dtype="datetime64[D]")
    calendar_days = calendar.to_numpy(dtype="datetime64[D]")
    entities = context.entity_id.astype(str).to_numpy()
    observed = pd.to_datetime(context.observation_time, utc=True).astype("int64").to_numpy()
    available = pd.to_datetime(context.feature_available_time, utc=True).astype("int64").to_numpy()
    decision_values = decisions.astype("int64").to_numpy()
    feature_counts = context.feature_count.to_numpy()
    lineage = context.feature_lineage_hash.to_numpy()
    missing_time = np.iinfo(np.int64).min
    for offset in range(0, len(members), 65536):
        stop = offset + 65536
        samples = member_targets[offset:stop]
        locations = member_context[offset:stop]
        expected_days = calendar_days[target_calendar_positions[samples] - step_len + 1 + steps[offset:stop]]
        if (not np.array_equal(actual_days[locations], expected_days)
                or not np.array_equal(entities[locations], target_entities[samples])):
            raise QlibModelError("序列模型 窗口会话或证券不一致")
        if ((feature_counts[locations] != len(columns)).any()
                or (observed[locations] == missing_time).any() or (available[locations] == missing_time).any()
                or (observed[locations] > available[locations]).any()
                or (available[locations] > decision_values[samples]).any()
                or pd.isna(lineage[locations]).any()):
            raise QlibModelError("序列模型 历史特征不完整或在末端决策时点尚不可见")
    # 紧凑成员只引用同一context行，时间与lineage不再复制；原始来源完整性由上游工件核对。
    current_locations = context_keys.get_indexer(pd.MultiIndex.from_arrays([target_days, target_entities]))
    if (current_locations < 0).any():
        raise QlibModelError("序列模型 末端缺少特征上下文")
    for offset in range(0, len(frame), 8192):
        stop = offset + 8192
        actual = context.iloc[current_locations[offset:stop]].loc[:, columns].to_numpy(dtype=float)
        expected = frame.iloc[offset:stop].loc[:, columns].to_numpy(dtype=float)
        if not np.array_equal(actual, expected, equal_nan=True):
            raise QlibModelError("序列模型 末端特征值与上下文不一致")
    used_positions = np.unique(member_context)
    ordered_positions = used_positions[context_keys.take(used_positions).argsort()]
    all_ordered = np.array_equal(ordered_positions, np.arange(len(context)))
    # 校验完成后只建立列结构；完整原序复用只读列，重排时每列仅take一次。
    selected_context = pd.DataFrame({
        column: (context[column].array if all_ordered
                 else context[column].array.take(ordered_positions))
        for column in context.columns
    }, copy=False)
    return replace(windows, context=selected_context, targets=bound.reset_index(drop=True),
                   members=members, exclusions=windows.exclusions.iloc[:0].copy())


def processed_dataset(windows, *, processors, segments, label_frame=None):
    """处理器已由合格 train 末端拟合；历史特征不受 DropnaLabel 删除影响。"""
    columns = windows.feature_columns
    source_index = pd.MultiIndex.from_frame(windows.context[["observation_session", "entity_id"]])
    index = pd.MultiIndex.from_arrays([pd.to_datetime(source_index.get_level_values(0)),
                                       source_index.get_level_values(1)], names=["datetime", "instrument"])
    # 处理器输入拥有独立数值列，元数据索引无需复制整个context。
    data = pd.DataFrame({("feature", column): windows.context[column].to_numpy(dtype=float, copy=True)
                         for column in columns}, index=index, copy=False)
    for processor in processors["infer"]:
        data = processor(data)
    if not data.index.equals(index) or list(data["feature"].columns) != list(columns):
        raise QlibModelError("序列模型 特征处理器改变了历史索引或特征顺序")
    values = data["feature"].to_numpy(dtype=float)
    if not np.isfinite(values).all() or np.any(np.abs(values) > np.finfo(np.float32).max):
        raise QlibModelError("序列模型 处理后的历史特征必须为有限 float32 数值")
    transformed = pd.DataFrame(values, index=source_index, columns=columns, copy=False)
    del data, values
    labels = None
    if label_frame is not None:
        learned = qlib_matrix(label_frame, columns, label=True)
        original_index = learned.index
        for processor in processors["learn"]:
            learned = processor(learned)
        if not learned.index.equals(original_index):
            raise QlibModelError("序列模型 标签处理器不得删除合格末端")
        wanted = pd.MultiIndex.from_arrays([pd.to_datetime(label_frame.observation_session).dt.tz_localize(None),
                                            label_frame.entity_id.astype(str)], names=["datetime", "instrument"])
        target = learned[("label", "target")].reindex(wanted).to_numpy(float)
        if not np.isfinite(target).all() or np.any(np.abs(target) > np.finfo(np.float32).max):
            raise QlibModelError("序列模型 训练标签必须为有限 float32 数值")
        labels = pd.Series(target, index=label_frame.sample_id.astype(str))
        del learned, label_frame
    return qlib_sequence_dataset(windows, segments=segments, labels=labels, transformed_features=transformed)


def save_window_facts(root, bundle_path, windows):
    paths = {}
    for name in ("context", "targets", "members"):
        relative = f"{bundle_path}/sequence/{name}.parquet"
        path = Path(root) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        getattr(windows, name).to_parquet(path, index=False)
        paths[name] = relative
    return {"schema": "research.qlib-sequence-input.v2", "files": paths,
            "step_len": windows.step_len, "feature_columns": list(windows.feature_columns),
            "calendar_sessions": [str(day) for day in windows.calendar_sessions],
            "missing_policy": "complete_window", "batch_axis": "vector"}


def verify_loaded_model(root, config, model):
    import torch
    model_class = config["candidate"]["model"]["class"]
    network = model_network(model, model_class)
    gpu = config["effective_model_kwargs"].get("GPU", -1)
    expected_device = "cpu" if gpu < 0 else f"cuda:{gpu}"
    if not model.fitted or str(model.device) != expected_device:
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
    if model_class in {"GATs", "ProjectQlibModel"}:
        if batch_contract(config["candidate"]) == "daily_cross_section":
            names = tuple(name for name in names if name != "batch_size") + ("base_model", "model_path")
        defaults.update(base_model="GRU", model_path=None)
    if model_class == "ProjectQlibModel":
        from .project_model import restore_project_model
        actual_class = restore_project_model(root, config)
        if type(model) is not actual_class:
            raise QlibModelError("恢复项目模型类型与封存源码不一致")
    if config.get("network_structure") != repr(network):
        raise QlibModelError("恢复网络结构与封存配置不一致")
    for name in names:
        expected = kwargs.get(name, defaults.get(name))
        if name == "optimizer":
            expected = expected.lower()
        if getattr(model, name) != expected:
            raise QlibModelError(f"恢复的序列模型 {name} 与封存参数不一致")
    weights = torch.load(Path(root) / config["weights_path"], map_location="cpu", weights_only=True)
    actual = network.state_dict()
    if (set(weights) != set(actual) or any(not torch.equal(weights[name], actual[name].detach().cpu()) for name in weights)
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
    optimizer_class = torch.optim.Adam if kwargs.get("optimizer", "adam").lower() == "adam" else torch.optim.SGD
    if model_class != "TransformerModel" and type(model.train_optimizer) is not optimizer_class:
        raise QlibModelError("序列模型优化器与声明不一致")


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
