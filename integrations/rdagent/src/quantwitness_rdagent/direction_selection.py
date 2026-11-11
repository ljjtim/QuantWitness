"""依据已验证知识选择可执行的因子方向，复用上游动作上下文模板。"""
import json

from .native_backend import research_backend
from .factor_research import factor_space, factor_directions


def knowledge_view(records):
    """提案只消费研究事实，不接收文件路径或完整结果。"""
    fields = ("record_id", "kind", "status", "expression", "hypothesis", "reason", "metrics", "reflection", "limitations")
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
    space = factor_space(context)
    records = context["knowledge"]
    if not records:
        return {"action": "stop", "direction": "stop", "knowledge_refs": [], "reason": "no_eligible_knowledge"}
    from rdagent.oai.llm_utils import APIBackend
    from rdagent.utils.agent.tpl import T
    user = T("scenarios.qlib.prompts:action_gen.user").r(
        hypothesis_and_feedback=json.dumps({"history": context["history"], "knowledge": records,
            "objective": context["objective"], "development": context["development"]}, ensure_ascii=False),
        last_hypothesis_and_feedback=json.dumps(context["history"][-1:], ensure_ascii=False))
    system = ("依据正式开发记录选择研究方向。只返回JSON，字段action、direction、knowledge_refs、reason。"
              "action为factor或stop，direction从冻结候选方向中选择，停止时为stop。"
              "knowledge_refs引用实际采用的record_id，reason说明开发依据；不扩大数据、公式、预算或使用test/holdout。"
              + json.dumps(space, ensure_ascii=False))
    with research_backend(complete, context.get("max_output_tokens", 2048)):
        response = APIBackend().build_messages_and_create_chat_completion(user, system, json_mode=True)
    value = json.loads(response)
    if (not isinstance(value, dict) or set(value) != {"action", "direction", "knowledge_refs", "reason"}
            or not isinstance(value["reason"], str) or not value["reason"].strip()
            or (value["action"], value["direction"]) not in {
                *(("factor", direction) for direction in factor_directions(space)), ("stop", "stop")}):
        raise ValueError("方向选择只能使用已支持的因子方向或stop")
    validate_references(value["knowledge_refs"], records)
    return value
