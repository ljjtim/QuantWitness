"""依据已验证知识选择可执行的因子方向，复用上游动作上下文模板。"""
import json

from .native_backend import research_backend
from .factor_research import DAI_FACTOR_CONTRACT, DAI_EXPRESSION_CONTRACT, factor_contract, factor_directions


def knowledge_view(records):
    """提案只消费研究事实，不接收文件路径或完整结果。"""
    fields = ("record_id", "kind", "status", "expression", "hypothesis", "reason", "metrics", "reflection")
    output = []
    for row in records:
        if row.get("usable_for_proposal") is not True:
            continue
        view = {key: row[key] for key in fields if key in row and row[key] is not None}
        view["status"] = "evaluated" if "metrics" in row else "failed"
        if view["status"] == "failed" or not view.get("reflection"):
            view.pop("reflection", None)
        output.append(view)
    return output


def validate_references(refs, records):
    allowed = {row["record_id"] for row in records}
    if (not isinstance(refs, list) or not refs or any(not isinstance(ref, str) for ref in refs)
            or len(set(refs)) != len(refs) or not set(refs) <= allowed):
        raise ValueError("knowledge_refs必须引用本次检索到的知识记录")


def select_direction(context, complete):
    """按已确认合同选择因子方向或停止。"""
    contract = factor_contract(context)
    records = context["knowledge"]
    if not records:
        return {"action": "stop", "direction": "stop", "knowledge_refs": [], "reason": "no_eligible_knowledge"}
    from rdagent.oai.llm_utils import APIBackend
    from rdagent.utils.agent.tpl import T
    user = T("scenarios.qlib.prompts:action_gen.user").r(
        hypothesis_and_feedback=json.dumps({"history": context["history"], "knowledge": records,
            "objective": context["objective"], "development": context["development"]}, ensure_ascii=False),
        last_hypothesis_and_feedback=json.dumps(context["history"][-1:], ensure_ascii=False))
    system = ('依据已验证开发记录选择下一研究方向。当前仅支持因子研究。'
              '只返回JSON，字段action、direction、knowledge_refs、reason。'
              'action为factor或stop；direction为momentum、mean_deviation、relative_volatility、range_position或stop。'
              'momentum使用close/Ref(close,N)-1，mean_deviation使用close/Mean(close,N)-1，N为1至5。'
              'relative_volatility使用Std(close,N)/Mean(close,N)，Std采用样本标准差；'
              'range_position使用(close-Min(close,N))/(Max(close,N)-Min(close,N))，平坦区间缺失，后两类N为2至5。'
              'knowledge_refs必须包含实际采用的知识record_id，reason说明证据如何支持选择。'
              '没有新方向或证据不足时返回stop，不扩大数据、公式或预算。')
    if contract == DAI_FACTOR_CONTRACT:
        system = ("依据已验证开发记录选择已确认日级因子方向。"
                  "只返回JSON，字段action、direction、knowledge_refs、reason。"
                  "action为factor或stop；factor方向只允许lag或mean，stop方向为stop。"
                  + DAI_EXPRESSION_CONTRACT +
                  "knowledge_refs引用实际采用的record_id，reason说明开发依据；不扩大输入、预算或使用test/holdout。")
    with research_backend(complete, context.get("max_output_tokens", 2048)):
        response = APIBackend().build_messages_and_create_chat_completion(user, system, json_mode=True)
    value = json.loads(response)
    if (not isinstance(value, dict) or set(value) != {"action", "direction", "knowledge_refs", "reason"}
            or not isinstance(value["reason"], str) or not value["reason"].strip()
            or (value["action"], value["direction"]) not in {
                *(("factor", direction) for direction in factor_directions(contract)), ("stop", "stop")}):
        raise ValueError("方向选择只能使用已支持的因子方向或stop")
    validate_references(value["knowledge_refs"], records)
    return value
