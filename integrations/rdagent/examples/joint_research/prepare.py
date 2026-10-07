"""准备三轮联合开发研究，模型特征引用正式评价过的因子。"""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path


def build(output, *, model_env_file=None):
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.joint_research import validate_joint_research
    base = Path(__file__).resolve().parents[1]
    def module(name):
        spec = importlib.util.spec_from_file_location("joint_" + name, base / name / "prepare.py")
        value = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(value)
        return value
    root = Path(output).resolve()
    model_module = module("model_research")
    model_module.build(root, repair_example=False, model_env_file=model_env_file)
    model = json.loads((root / "request.json").read_text(encoding="utf-8"))
    def reflection(observation, model=False):
        return {"Observations": observation, "Feedback for Hypothesis": "仅采用已验证开发指标", "New Hypothesis": "比较因子窗口与模型非线性",
                "Reasoning": "使用相同冻结开发范围", "Decision" if model else "Replace Best Result": False}
    responses = [reflection("文档因子基线已评价"),
        {"action": "factor", "direction": "momentum", "knowledge_refs": ["synthetic_joint_research:0000"], "reason": "基线反思支持比较较短窗口"},
        {"hypothesis": "短期动量可能改变开发误差", "reason": "比较同范围内的新历史窗口"},
        {"short_momentum": {"description": "三期动量", "formulation": "close/Ref(close,3)-1", "variables": {"close": "前一完整会话收盘价"}, "expression": "$close / Ref($close, 3) - 1"}},
        reflection("新因子完成独立开发验证"),
        {"action": "model", "direction": "generated_model", "knowledge_refs": ["synthetic_joint_research:0000", "synthetic_joint_research:0001"], "reason": "以已验证因子和窗口比较结果检验非线性模型"},
        {"hypothesis": "非线性跳连可改变已验证因子的预测误差", "reason": "沿用因子分支的开发反馈"},
        {"definition": model_module.NETWORK}, reflection("模型已按正式开发结果评价", True)]
    if not model_env_file:
        paths = []
        for index, response in enumerate(responses):
            path = root / "joint-responses" / f"{index:02d}.json"
            write_json(path, response)
            paths.append(str(path))
        proposer = {"mode": "fixed_responses", "responses": paths}
    else:
        proposer = model["proposer"]
    budget = {"rounds": 3, "evaluations": 3, "model_calls": 12, "output_tokens": 24576, "max_output_tokens_per_call": 2048}
    model.update(proposer=proposer)
    factor = {"contract_version": "rd-factor-research-v1", "research_kind": "factor_research", "campaign_id": "joint_factor",
        "session_root": str(root / "factor-placeholder"), "package_template": deepcopy(model["package_template"]),
        "confirmed_spec": {"kind": "synthetic", "path": str(root / "confirmed-definition.json")},
        "baseline": {"expression": module("factor_research").BASELINE_EXPRESSION, "hypothesis": "文档五期动量基线", "reason": "先复现已确认定义"},
        "budget": {key: value for key, value in budget.items() if key != "evaluations"}, "proposer": proposer}
    payload = {"contract_version": "rd-joint-research-v1", "research_kind": "joint_research", "campaign_id": "synthetic_joint_research",
        "session_root": str(root / "joint-session"), "factor_request": factor, "model_request": model, "budget": budget, "proposer": proposer}
    validate_joint_research(payload)
    write_json(root / "request.json", payload)
    return {"status": "prepared", "request": str(root / "request.json"), "model_calls": 0, "database_used": False}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model-env-file")
    args = parser.parse_args()
    print(json.dumps(build(args.output, model_env_file=args.model_env_file), ensure_ascii=False))
