"""生成前馈图的声明与确定性源码编译，不加载可选模型依赖。"""
from copy import deepcopy
import math

from .inputs import QlibModelError

ACTIVATIONS = {"identity": "Identity", "relu": "ReLU", "tanh": "Tanh", "gelu": "GELU"}


def validate_definition(definition):
    if not isinstance(definition, dict) or set(definition) != {"nodes"}:
        raise QlibModelError("生成网络必须声明nodes")
    nodes = definition["nodes"]
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 8:
        raise QlibModelError("生成网络需要1至8个节点")
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or set(node) != {"inputs", "width", "activation"}:
            raise QlibModelError("生成节点必须声明inputs/width/activation")
        inputs = node["inputs"]
        if (not isinstance(inputs, list) or not inputs or len(inputs) != len(set(inputs))
                or any(type(value) is not int or not -1 <= value < index for value in inputs)):
            raise QlibModelError("生成网络输入只能引用原特征(-1)或此前节点")
        if type(node["width"]) is not int or not 1 <= node["width"] <= 64:
            raise QlibModelError("生成网络节点宽度必须在1至64之间")
        if node["activation"] not in ACTIVATIONS:
            raise QlibModelError("生成网络激活函数不受支持")
    if nodes[-1]["width"] != 1 or nodes[-1]["activation"] != "identity":
        raise QlibModelError("生成回归网络末节点必须为线性标量")
    used = {len(nodes) - 1}
    for index in reversed(range(len(nodes))):
        if index in used:
            used.update(value for value in nodes[index]["inputs"] if value >= 0)
    if used != set(range(len(nodes))):
        raise QlibModelError("生成网络不能包含不参与输出的节点")
    return deepcopy(definition)


def validate_kwargs(kwargs):
    required = {"definition", "epochs", "learning_rate", "early_stop", "l2"}
    if set(kwargs) != required:
        raise QlibModelError("生成模型须完整声明结构、训练轮数、学习率、早停和l2")
    validate_definition(kwargs["definition"])
    if type(kwargs["epochs"]) is not int or not 1 <= kwargs["epochs"] <= 100:
        raise QlibModelError("生成模型epochs必须为1至100")
    if type(kwargs["early_stop"]) is not int or not 1 <= kwargs["early_stop"] <= kwargs["epochs"]:
        raise QlibModelError("生成模型early_stop必须在训练轮数范围内")
    for key in ("learning_rate", "l2"):
        value = kwargs[key]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise QlibModelError("生成模型训练参数必须有限非负")
    if not 0 < kwargs["learning_rate"] <= 1:
        raise QlibModelError("生成模型学习率必须大于0且不超过1")


def compile_source(definition, d_feat):
    definition = validate_definition(definition)
    if type(d_feat) is not int or d_feat < 1:
        raise QlibModelError("生成模型特征维度无效")
    widths = [d_feat] + [node["width"] for node in definition["nodes"]]
    lines = ["import torch", "from torch import nn", "", "class ResearchNetwork(nn.Module):",
             "    def __init__(self):", "        super().__init__()"]
    for index, node in enumerate(definition["nodes"]):
        size = sum(widths[source + 1] for source in node["inputs"])
        lines.append(f"        self.layer_{index} = nn.Linear({size}, {node['width']})")
        lines.append(f"        self.activation_{index} = nn.{ACTIVATIONS[node['activation']]}()")
    lines += ["", "    def forward(self, x):"]
    for index, node in enumerate(definition["nodes"]):
        values = ["x" if source == -1 else f"n{source}" for source in node["inputs"]]
        joined = values[0] if len(values) == 1 else "torch.cat((" + ", ".join(values) + "), dim=1)"
        lines.append(f"        n{index} = self.activation_{index}(self.layer_{index}({joined}))")
    lines.append(f"        return n{len(definition['nodes']) - 1}.reshape(-1)")
    return "\n".join(lines) + "\n"


