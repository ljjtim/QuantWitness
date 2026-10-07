"""联合研究只向上游组件提供已验证开发事实。"""
import math

from .factor_research import DAI_FACTOR_CONTRACT, factor_contract


def validate_joint_feedback(records):
    fields = {"record_id", "kind", "candidate_id", "hypothesis", "reason", "expression", "definition", "status", "metrics", "reflection"}
    if not isinstance(records, list):
        raise ValueError("联合知识必须为开发记录列表")
    for row in records:
        if not isinstance(row, dict) or set(row) - fields or row.get("status") != "evaluated":
            raise ValueError("联合知识只接受已验证开发记录")
        if row.get("kind") not in {"factor", "model"} or not isinstance(row.get("record_id"), str) or not row["record_id"]:
            raise ValueError("联合知识缺少分支或引用")
        metric = row.get("metrics")
        if (not isinstance(metric, dict) or set(metric) != {"value", "rows"}
                or type(metric["value"]) not in (int, float) or not math.isfinite(metric["value"])
                or type(metric["rows"]) is not int or metric["rows"] < 1):
            raise ValueError("联合知识缺少有限开发指标")
    return records


def development_population(model):
    """共同资格由正式样本、标签时点和validation成员决定。"""
    if model.get("mode") != "walk_forward_development_v1":
        raise ValueError("联合评价仅接受正式开发样本")
    tables = model["tables"]
    if set(tables) & {"test_predictions", "holdout_predictions", "holdout_receipt"}:
        raise ValueError("联合评价不得读取最终留出事实")
    contract = factor_contract(model.get("design", {}))
    prefixes = ("dai_following__",) if contract == DAI_FACTOR_CONTRACT else ("historical_return__", "volatility__")
    samples = []
    for row in tables["samples"]:
        feature_values = [value for key, value in row.items() if key.startswith(prefixes) or (contract == DAI_FACTOR_CONTRACT and key == "dai_following")]
        if not feature_values or any(type(value) not in (int, float) or not math.isfinite(value) for value in feature_values):
            raise ValueError("联合评价要求冻结特征均有完整值")
        samples.append({key: row[key] for key in ("sample_id", "target", "label_start_time", "label_end_time", "label_available_time")})
    predictions = {}
    for row in tables["validation_predictions"]:
        key = (row["fold_id"], row["sample_id"])
        value = {name: row[name] for name in ("fold_id", "sample_id", "actual", "label_end_time", "label_available_time")}
        if key in predictions and predictions[key] != value:
            raise ValueError("联合validation同一样本的标签事实不同")
        predictions[key] = value
    return {"samples": sorted(samples, key=lambda row: row["sample_id"]),
            "validation": [predictions[key] for key in sorted(predictions)]}
