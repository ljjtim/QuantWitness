"""研究包编译时核对序列切分与候选的公开合同。"""
from __future__ import annotations

import json

from research_pipeline.platform.operator_contracts import OperatorGraphRecipe

from .models import ResearchPackageError


def validate_model_sequence_graph(recipe: OperatorGraphRecipe) -> None:
    """在启动 Worker 前固定窗口长度与所有候选的标签口径。"""
    nodes = {node.node_id: node for node in recipe.nodes}
    split_steps: dict[str, int] = {}
    model_nodes = []
    for node in recipe.nodes:
        if node.operator_id == "research.model.split-manifest":
            if "sequence_step_len" in node.parameters:
                step = node.parameters["sequence_step_len"]
                if type(step) is not int or step < 2:
                    raise ResearchPackageError("sequence_step_len 必须是至少2的整数")
                split_steps[node.node_id] = step
        elif node.operator_id in {"research.model.fit", "research.model.locked-holdout"}:
            model_nodes.append(node)
    consumed_sequence_splits: set[str] = set()
    for node in model_nodes:
        parameters = node.parameters
        if parameters.get("thread_count") != 1:
            raise ResearchPackageError(f"{node.operator_id} 只允许 thread_count=1，以保证确定性")
        from research_pipeline.research.modeling.qlib import normalize_candidates

        try:
            raw = parameters.get("candidate_jsons")
            if not isinstance(raw, (tuple, list)) or not raw:
                raise ValueError("candidate_jsons 必须是非空列表")
            candidates = normalize_candidates([json.loads(value) for value in raw])
        except (ValueError, TypeError, KeyError) as exc:
            raise ResearchPackageError(f"Qlib 模型候选声明无效: {exc}") from exc
        lengths = {item["dataset"]["step_len"] for item in candidates
                   if item["model"]["class"] in {"GRU", "LSTM", "TransformerModel"}}
        split_binding = next((binding for binding in node.inputs
                              if binding.input_port == "splits"), None)
        source = nodes.get(split_binding.source_node_id) if split_binding else None
        step = split_steps.get(source.node_id) if source else None
        if lengths:
            if (source is None or source.operator_id != "research.model.split-manifest"
                    or split_binding.source_output_port != "splits" or lengths != {step}):
                raise ResearchPackageError("全部序列候选必须匹配split冻结的sequence_step_len")
        if step is not None:
            if not lengths:
                raise ResearchPackageError("序列切分至少需要一个匹配窗口的序列候选")
            if any(processor["class"] == "CSRankNorm" for candidate in candidates
                   for processor in candidate["processors"]["learn"]):
                raise ResearchPackageError("正式序列模型评价要求raw标签，不支持CSRankNorm")
            consumed_sequence_splits.add(source.node_id)
    if set(split_steps) - consumed_sequence_splits:
        raise ResearchPackageError("序列切分必须供至少一个序列模型节点使用")
