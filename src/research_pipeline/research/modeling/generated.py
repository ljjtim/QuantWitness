"""运行时提出的前馈图、Qlib数据接口与可封存网络源码。"""
from __future__ import annotations

from copy import deepcopy
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from qlib.model.base import Model

from .inputs import QlibModelError

from .generated_definition import compile_source, validate_definition, validate_kwargs


def _network(source):
    namespace = {}
    exec(compile(source, "<research-network>", "exec"), namespace)
    return namespace["ResearchNetwork"]().double()


class GeneratedModel(Model):
    """训练仅接收Qlib开发分段；保存数值权重，不pickle动态类。"""

    def __init__(self, definition, epochs, learning_rate, early_stop, l2, d_feat, seed):
        validate_kwargs(dict(definition=definition, epochs=epochs, learning_rate=learning_rate,
                             early_stop=early_stop, l2=l2))
        self.definition = deepcopy(definition)
        self.epochs, self.learning_rate, self.early_stop, self.l2 = epochs, learning_rate, early_stop, l2
        self.d_feat, self.seed = d_feat, seed
        self.source = compile_source(definition, d_feat)
        self.weights = {}
        self.curve = []

    def fit(self, dataset):
        import torch
        from qlib.data.dataset.handler import DataHandlerLP
        from .gru import cpu_execution
        train, valid = dataset.prepare(["train", "valid"], col_set=["feature", "label"], data_key=DataHandlerLP.DK_L)
        frames = []
        for frame in (train, valid):
            x, y = frame["feature"].to_numpy(dtype=float), frame["label"].iloc[:, 0].to_numpy(dtype=float)
            if len(x) == 0 or x.shape[1] != self.d_feat or not np.isfinite(x).all() or not np.isfinite(y).all():
                raise QlibModelError("生成模型需要完整有限训练/验证样本")
            frames.append((torch.as_tensor(x, dtype=torch.float64), torch.as_tensor(y, dtype=torch.float64)))
        with cpu_execution(), torch.random.fork_rng(devices=[]):
            torch.manual_seed(self.seed)
            network = _network(self.source)
            optimizer = torch.optim.Adam(network.parameters(), lr=self.learning_rate, weight_decay=self.l2)
            best, stale = float("inf"), 0
            self.curve = []
            for epoch in range(self.epochs):
                network.train()
                optimizer.zero_grad()
                loss = torch.mean((network(frames[0][0]) - frames[0][1]) ** 2)
                loss.backward()
                optimizer.step()
                network.eval()
                with torch.no_grad():
                    scores = [float(torch.mean((network(x) - y) ** 2)) for x, y in frames]
                if not all(math.isfinite(value) for value in scores):
                    raise QlibModelError("生成模型训练出现非有限损失")
                self.curve.extend({"segment": segment, "metric": "mse", "iteration": epoch, "value": value}
                                  for segment, value in zip(("train", "valid"), scores))
                if scores[1] < best:
                    best, stale = scores[1], 0
                    self.weights = {name: value.detach().cpu().numpy().copy() for name, value in network.state_dict().items()}
                else:
                    stale += 1
                if stale >= self.early_stop:
                    break

    def predict(self, dataset, segment="test"):
        import torch
        from qlib.data.dataset.handler import DataHandlerLP
        from .gru import cpu_execution
        frame = dataset.prepare(segment, col_set="feature", data_key=DataHandlerLP.DK_I)
        x = frame.to_numpy(dtype=float)
        if x.shape[1] != self.d_feat or not np.isfinite(x).all() or not self.weights:
            raise QlibModelError("生成模型预测输入或拟合状态无效")
        with cpu_execution(), torch.random.fork_rng(devices=[]):
            network = _network(self.source)
            network.load_state_dict({name: torch.as_tensor(value) for name, value in self.weights.items()})
            network.eval()
            with torch.no_grad():
                values = network(torch.as_tensor(x, dtype=torch.float64)).numpy()
        return pd.Series(values, index=frame.index)


def save_generated(root, bundle_path, model):
    root = Path(root)
    source = f"{bundle_path}/network.py"
    weights = f"{bundle_path}/network-weights.npz"
    (root / source).write_text(model.source, encoding="utf-8")
    np.savez(root / weights, **model.weights)
    return {"source_path": source, "weights_path": weights, "definition": model.definition,
            "d_feat": model.d_feat, "weight_shapes": {key: list(value.shape) for key, value in model.weights.items()}}


def verify_loaded(root, config, model):
    facts = config["generated"]
    source = (Path(root) / facts["source_path"]).read_text(encoding="utf-8")
    if source != compile_source(config["candidate"]["model"]["kwargs"]["definition"], len(config["feature_columns"])) or model.source != source:
        raise QlibModelError("生成网络源码与冻结结构不一致")
    if model.definition != facts["definition"] or model.d_feat != len(config["feature_columns"]):
        raise QlibModelError("生成网络对象与冻结配置不一致")
    with np.load(Path(root) / facts["weights_path"], allow_pickle=False) as values:
        if set(values.files) != set(model.weights) or any(not np.array_equal(values[name], model.weights[name]) for name in values.files):
            raise QlibModelError("生成网络权重与封存模型不一致")
