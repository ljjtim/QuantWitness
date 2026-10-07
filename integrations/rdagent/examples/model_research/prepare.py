"""准备合成数据上的运行时前馈模型研究；不训练或调用模型。"""
import importlib.util
import json
from pathlib import Path

BASELINE = {"nodes": [{"inputs": [-1], "width": 1, "activation": "identity"}]}
NETWORK = {"nodes": [{"inputs": [-1], "width": 4, "activation": "tanh"},
                     {"inputs": [-1, 0], "width": 1, "activation": "identity"}]}


def build(output, *, model_env_file=None, coding_sources=(), model_family="GeneratedModel", repair_example=True):
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.model_research import validate_model_research
    base = Path(__file__).resolve().parents[1] / "factor_research/prepare.py"
    spec = importlib.util.spec_from_file_location("model_base_prepare", base)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = Path(output).resolve()
    template = module.prepare_base(root)
    definition = root / "model-definition.md"
    definition.write_text("# 合成模型研究定义\n\n使用虚构ETF与冻结特征研究下一期价格变化。\n"
        "基线是单层线性网络，后续提出新的前馈连接与激活。\n"
        "训练设置固定；各轮仅评价独立验证通过的validation均方误差。\n", encoding="utf-8")
    def reflection(text):
        return {"Observations": text, "Feedback for Hypothesis": "结论以正式开发指标为准",
                "New Hypothesis": "检验带原输入跳连的非线性网络", "Reasoning": "比较不同结构的开发误差", "Decision": False}
    responses = [reflection("单层基线已经完成"),
                 {"hypothesis": "非线性隐层与原输入跳连可改变预测误差", "reason": "按基线反思检验新的结构"}]
    if repair_example:
        responses += [{"definition": {"nodes": [{"inputs": [0], "width": 1, "activation": "identity"}]}}]
    responses += [{"definition": NETWORK}, reflection("新结构已按正式结果评价")]
    if model_env_file:
        from quantwitness_rdagent.model_client import public_config
        proposer = {"mode": "live", **public_config(model_env_file)}
    else:
        files = []
        for index, response in enumerate(responses):
            path = root / "responses" / f"{index:02d}.json"
            write_json(path, response)
            files.append(str(path))
        proposer = {"mode": "fixed_responses", "responses": files}
    payload = {"contract_version": "rd-model-research-v1", "research_kind": "model_research",
        "campaign_id": "synthetic_generated_model", "session_root": str(root / "session"),
        "package_template": template, "confirmed_spec": str(definition),
        "baseline": {"definition": BASELINE, "hypothesis": "单层回归作为结构基线", "reason": "先建立可复核的简单模型"},
        "training": {"epochs": 4, "learning_rate": 0.02, "early_stop": 2, "l2": 0.001},
        "budget": {"rounds": 2, "model_calls": 5, "output_tokens": 10240, "max_output_tokens_per_call": 2048, "repairs": 1},
        "proposer": proposer,
        "coding_knowledge": {"source_sessions": [str(Path(p).resolve()) for p in coding_sources], "model_family": model_family}}
    validate_model_research(payload)
    write_json(root / "request.json", payload)
    return {"status": "prepared", "request": str(root / "request.json"), "model_calls": 0, "database_used": False}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model-env-file")
    parser.add_argument("--coding-source", action="append", default=[])
    parser.add_argument("--model-family", default="GeneratedModel")
    args = parser.parse_args()
    print(json.dumps(build(args.output, model_env_file=args.model_env_file, coding_sources=args.coding_source, model_family=args.model_family), ensure_ascii=False))
