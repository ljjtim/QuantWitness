"""准备教学文档的因子研究基包；不训练、不调用模型、不写数据库。"""
import importlib.util
import json
from pathlib import Path
import sys


BASELINE_EXPRESSION = "$close / Ref($close, 5) - 1"


def prepare_base(output):
    """返回单基线研究包请求，供研究循环生成后续表达式候选。"""
    root = Path(output).resolve()
    if root.exists():
        raise ValueError("输出目录必须尚不存在")
    example = Path(__file__).resolve().parents[4] / "examples/qlib_portfolio"
    sys.path.insert(0, str(example))
    spec = importlib.util.spec_from_file_location("qlib_factor_research_prepare", example / "prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.package_campaign import validate_package_campaign
    root.mkdir(parents=True)
    bundle = module.prepare(root / "input", "development", feature_expressions={
        "historical_return": BASELINE_EXPRESSION,
    })
    facts = json.loads((root / "input/request.json").read_text(encoding="utf-8"))
    archive = root / "source-archive"
    archive.mkdir()
    teaching_document = root / "confirmed-definition.md"
    teaching_document.write_text(
        "# 已确认的教学因子定义\n\n"
        "本研究使用虚构ETF行情，仅验证研究循环。\n"
        "决策时点为交易会话09:30，仅使用此前会话收盘价。\n"
        "基线为前一会话收盘相对五期前收盘的变化：$close/Ref($close,5)-1。\n"
        "表达式在前一会话收盘行求值；当前会话行情不能参与。\n"
        "historical_return 的两个既有窗口槽都使用同一表达式；volatility 保留原定义。\n"
        "后续改进另记为新假设，不改变原文复现定义。\n",
        encoding="utf-8",
    )
    write_json(root / "confirmed-definition.json", {
        "status": "confirmed", "confirmed_by": "example_definition", "kind": "synthetic",
        "title": "虚构ETF的收盘动量教学定义", "formula": BASELINE_EXPRESSION,
        "text": teaching_document.read_text(encoding="utf-8"),
    })
    payload = {
        "research_kind": "package", "campaign_id": "synthetic_factor_baseline",
        "session_root": str(root / "baseline-session"),
        "source": {"kind": "research_package", "path": bundle["package"],
            "source_archive_root": str(archive), "input_snapshot_manifest": bundle["input_snapshot_manifest"],
            "catalog_lock": bundle["catalog_lock"], "verifier_bundle": bundle["verifier"],
            "extension_bundles": bundle["extensions"], "runtime_options": {"workers": 1},
            "verification_process_slots": 3 if sys.platform == "win32" else 2,
            "verification_memory_bytes": 4 * 1024 ** 3},
        "development": {"start": facts["design"]["research_sessions"][0],
            "end": facts["design"]["calendar_sessions"][78], "as_of": bundle["fixed_clock"]},
        "objective": {"table_id": "metrics", "schema_id": "project.qlib_demo.metrics.v1",
            "value_column": "value", "date_column": "session", "availability_column": "available_at",
            "stage_column": "stage", "filters": {}, "reduction": "mean", "direction": "minimize"},
        "candidates": [{"id": "baseline", "parameter_overrides": [{"node_id": "feature", "parameter_name": "design", "value": facts["design"]}]}], "baseline_id": "baseline",
        "budget": {"rounds": 1, "evaluations": 1, "model_calls": 0, "output_tokens": 0,
            "max_output_tokens_per_call": 0, "max_rows": 10, "memory_bytes": 16777216},
        "stop": {"target_value": None, "min_improvement": 0, "patience": 1},
        "proposer": {"mode": "fixed_policy"},
    }
    validate_package_campaign(payload)
    write_json(root / "package-template.json", payload)
    return payload


def knowledge_responses(records, reflect):
    """教学响应引用实际记录；固定选择仅验证接线，不代表自主研究。"""
    from quantwitness_rdagent.factor_research import canonical_expression, expression_direction
    eligible = [row for row in records if row.get("usable_for_proposal") is True and "metrics" in row]
    if not eligible:
        raise ValueError("教学知识例需要至少一条已验证开发指标记录")
    refs = [eligible[-1]["record_id"]]
    studied = {canonical_expression(row["expression"]) for row in records if row.get("expression")}
    choices = [f"$close / {operator}($close, {window}) - 1"
               for operator in ("Mean", "Ref") for window in (4, 2, 1, 3, 5)]
    choices += [f"Std($close, {window}) / Mean($close, {window})" for window in (3, 4, 2, 5)]
    choices += [f"($close - Min($close, {window})) / (Max($close, {window}) - Min($close, {window}))"
                for window in (3, 4, 2, 5)]
    expression = next((value for value in choices if value not in studied | {BASELINE_EXPRESSION}), None)
    if expression is None:
        raise ValueError("教学表达式范围已研究完毕；需新范围或停止研究")
    direction = expression_direction(expression)
    return [reflect("已完成教学基线", "依据上一会话的已验证结果选择新方向"),
        {"action": "factor", "direction": direction, "knowledge_refs": refs,
         "reason": "依据上一会话的正式开发记录，检验尚未研究的窗口"},
        {"hypothesis": "改变历史窗口可能影响开发预测误差", "reason": "使用同一开发范围检验窗口变化"},
        {"next_factor": {"description": "不同历史窗口的价格特征", "formulation": expression,
            "variables": {"close": "前一完整会话收盘价"}, "expression": expression, "knowledge_refs": refs}},
        reflect("实验结果以正式开发指标为准", "保留结果并按剩余研究预算选择方向")]


def build(output, *, model_env_file=None, knowledge_index=None, knowledge_output=None, campaign_id="synthetic_factor_research"):
    """固定文本验收或显式live研究；响应按调用顺序保存，不作为候选菜单。"""
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.factor_research import validate_factor_research
    template = prepare_base(output)
    root = Path(output).resolve()
    responses = []
    def reflect(observation, next_idea):
        return {"Observations": observation, "Feedback for Hypothesis": "仅评价本轮已验证开发指标",
                "New Hypothesis": next_idea, "Reasoning": "比较相同开发范围，负结果同样保留", "Replace Best Result": False}
    fixed = [reflect("已完成教学基线", "检验较短动量"),
        {"hypothesis": "短期动量对开发样本的预测可能不同", "reason": "根据基线反思检验更短历史"},
        {"short_momentum": {"description": "三期动量", "formulation": "close/Ref(close,3)-1",
            "variables": {"close": "前一完整会话收盘价"}, "expression": "$close / Ref($close, 3) - 1"}},
        reflect("短动量实验已执行，结论以正式指标为准", "检验相对均价偏离"),
        {"hypothesis": "相对近期均价的偏离可提供不同特征", "reason": "延续前轮反思，比较平滑基准"},
        {"mean_deviation": {"description": "三期均价偏离", "formulation": "close/Mean(close,3)-1",
            "variables": {"close": "前一完整会话收盘价"}, "expression": "$close / Mean($close, 3) - 1"}},
        reflect("完成平滑基准实验", "当前教学预算结束，保留后续研究方向")]
    if bool(knowledge_index) != bool(knowledge_output):
        raise ValueError("知识输入和输出路径须同时提供")
    if knowledge_index and not model_env_file:
        records = json.loads(Path(knowledge_index).read_text(encoding="utf-8"))["records"]
        fixed = knowledge_responses(records, reflect)
    if model_env_file:
        from quantwitness_rdagent.model_client import public_config
        proposer = {"mode": "live", **public_config(model_env_file)}
    else:
        for i, value in enumerate(fixed):
            path = root / "responses" / f"{i:02d}.json"
            write_json(path, value)
            responses.append(str(path))
        proposer = {"mode": "fixed_responses", "responses": responses}
    payload = {"contract_version": "rd-factor-research-v1", "research_kind": "factor_research",
        "campaign_id": campaign_id, "session_root": str(root / "session"),
        "package_template": template, "confirmed_spec": {"kind": "synthetic", "path": str(root / "confirmed-definition.json")},
        "baseline": {"expression": BASELINE_EXPRESSION, "hypothesis": "按教学文档复现五期收盘动量", "reason": "先完成固定定义的正式基线"},
        "budget": {"rounds": 2 if knowledge_index else 3, "model_calls": 5 if knowledge_index else 7,
            "output_tokens": 10240 if knowledge_index else 14336, "max_output_tokens_per_call": 2048},
        "proposer": proposer}
    if knowledge_index:
        payload["knowledge"] = {"index": str(Path(knowledge_index).resolve()),
            "output": str(Path(knowledge_output).resolve()), "max_records": 8}
    validate_factor_research(payload)
    write_json(root / "request.json", payload)
    return {"status": "prepared", "request": str(root / "request.json"), "database_used": False,
            "model_calls": 0, "input_kind": "synthetic"}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model-env-file", help="读取模型公开身份生成live请求；本命令不调用模型")
    parser.add_argument("--knowledge-index", help="上一正式开发会话导出的研究知识索引")
    parser.add_argument("--knowledge-output", help="本会话完成后写出的研究知识索引")
    parser.add_argument("--campaign-id", default="synthetic_factor_research", help="独立研究会话的唯一名称")
    args = parser.parse_args()
    print(json.dumps(build(args.output, model_env_file=args.model_env_file,
        knowledge_index=args.knowledge_index, knowledge_output=args.knowledge_output, campaign_id=args.campaign_id), ensure_ascii=False))
