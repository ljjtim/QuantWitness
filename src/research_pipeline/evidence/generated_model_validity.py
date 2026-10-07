"""只读Result源码和数值权重，独立复核生成网络连接与维度。"""
from __future__ import annotations

import ast
import io

import numpy as np

from .errors import EvidenceContractError


def _require(condition, message):
    if not condition:
        raise EvidenceContractError(message)


def verify_source_and_weights(config, source, weights):
    definition = config["candidate"]["model"]["kwargs"]["definition"]
    nodes = definition["nodes"]
    facts = config["generated"]
    width = len(config["feature_columns"])
    _require(facts["definition"] == definition and facts["d_feat"] == width, "生成网络结构或特征维度不一致")
    module = ast.parse(source)
    _require(len(module.body) == 3, "生成网络源码包含额外执行语句")
    _require(ast.dump(module.body[0]) == ast.dump(ast.parse("import torch").body[0])
             and ast.dump(module.body[1]) == ast.dump(ast.parse("from torch import nn").body[0]), "生成网络依赖声明不符")
    network = module.body[2]
    _require(isinstance(network, ast.ClassDef) and network.name == "ResearchNetwork"
             and len(network.bases) == 1 and ast.unparse(network.bases[0]) == "nn.Module"
             and not network.decorator_list and not network.keywords and len(network.body) == 2, "生成网络类结构不符")
    constructor, forward = network.body
    _require(isinstance(constructor, ast.FunctionDef) and constructor.name == "__init__"
             and ast.unparse(constructor.args) == "self" and not constructor.decorator_list
             and not constructor.returns and len(constructor.body) == 1 + 2 * len(nodes), "生成网络初始化结构不符")
    _require(ast.dump(constructor.body[0]) == ast.dump(ast.parse("super().__init__()").body[0]), "生成网络初始化入口不符")
    _require(isinstance(forward, ast.FunctionDef) and forward.name == "forward"
             and ast.unparse(forward.args) == "self, x" and not forward.decorator_list
             and not forward.returns and len(forward.body) == len(nodes) + 1, "生成网络预测结构不符")
    dims, expected_weights = {-1: width}, {}
    activations = {"identity": "Identity", "relu": "ReLU", "tanh": "Tanh", "gelu": "GELU"}
    for index, node in enumerate(nodes):
        _require(node["inputs"] and all(type(parent) is int and parent in dims for parent in node["inputs"]), "生成网络包含未定义连接")
        count = sum(dims[parent] for parent in node["inputs"])
        dims[index] = node["width"]
        linear = f"self.layer_{index} = nn.Linear({count}, {node['width']})"
        activation = f"self.activation_{index} = nn.{activations[node['activation']]}()"
        for actual, text in zip(constructor.body[1 + index * 2:3 + index * 2], (linear, activation)):
            _require(ast.dump(actual) == ast.dump(ast.parse(text).body[0]), "生成网络层参数与声明不一致")
        args = ["x" if parent == -1 else f"n{parent}" for parent in node["inputs"]]
        value = args[0] if len(args) == 1 else "torch.cat((" + ", ".join(args) + "), dim=1)"
        expected = f"n{index} = self.activation_{index}(self.layer_{index}({value}))"
        _require(ast.dump(forward.body[index]) == ast.dump(ast.parse(expected).body[0]), "生成网络计算连接与声明不一致")
        expected_weights[f"layer_{index}.weight"] = [node["width"], count]
        expected_weights[f"layer_{index}.bias"] = [node["width"]]
    _require(dims[len(nodes) - 1] == 1 and nodes[-1]["activation"] == "identity", "生成模型不是标量回归")
    _require(ast.dump(forward.body[-1]) == ast.dump(ast.parse(f"return n{len(nodes)-1}.reshape(-1)").body[0]), "生成网络输出与声明不一致")
    _require(facts["weight_shapes"] == expected_weights, "生成网络权重维度声明不符")
    with np.load(io.BytesIO(weights), allow_pickle=False) as arrays:
        _require(set(arrays.files) == set(expected_weights), "生成模型封存权重不完整")
        for name, shape in expected_weights.items():
            _require(list(arrays[name].shape) == shape and np.isfinite(arrays[name]).all(), "生成模型封存权重形状或数值无效")


def verify_generated_support(snapshot, artifact_key, config):
    sources = {(item.artifact_key, item.source_path): item for item in snapshot.bundle.support_files}
    payloads = {}
    for name in ("source_path", "weights_path"):
        item = sources.get((artifact_key, config["generated"][name]))
        _require(item is not None and item.relative_path in snapshot.support_bytes, "生成模型缺少已验证源码或权重")
        payloads[name] = snapshot.support_bytes[item.relative_path]
    verify_source_and_weights(config, payloads["source_path"].decode("utf-8"), payloads["weights_path"])
