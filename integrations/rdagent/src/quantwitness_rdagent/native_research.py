"""固定 RD-Agent 的因子提案、实验定义及反思，输入限于正式开发视图。

propose_factor(context, complete)进行两次请求：假设、实验定义。
reflect_factor(context, complete)进行一次请求：研究反思。
complete(prompt, *, instructions, max_output_tokens)返回文本或含text的字典。
context包含confirmed_spec、fields、max_window、expression_contract、history，
max_output_tokens可显式限制单次响应；反思另传current。history/current由主循环
从已验证开发结果构建，仅传metrics.value及metrics.rows，不传Result正文、日志
或最终隔离集。此模块不执行研究或替代主循环的独立验证。
"""
from __future__ import annotations

import json

from .native_backend import research_backend


_CONTEXT_FIELDS = {
    "confirmed_spec", "fields", "max_window", "expression_contract",
    "history", "max_output_tokens", "current", "development", "objective",
    "knowledge", "direction", "joint_knowledge", "joint_direction", "factor_contract",
}
_RECORD_FIELDS = {"candidate_id", "hypothesis", "reason", "expression", "metrics", "reflection", "status"}
_REFLECTION_FIELDS = {"observations", "hypothesis_evaluation", "new_hypothesis", "reason", "decision"}
_HYPOTHESIS_FIELDS = (
    "hypothesis", "reason", "concise_reason", "concise_observation",
    "concise_justification", "concise_knowledge",
)


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name}必须为非空文本")
    return value


def _record(value):
    if not isinstance(value, dict) or set(value) - _RECORD_FIELDS:
        raise ValueError("研究历史必须使用开发记录字段")
    status = value.get("status", "evaluated")
    if status in {"failed", "rejected", "stopped"}:
        if "metrics" in value or "reflection" in value:
            raise ValueError("技术失败或未执行候选不能携带金融结果和研究反思")
        return value
    if status != "evaluated":
        raise ValueError("研究历史状态无效")
    metrics = value.get("metrics")
    if not isinstance(metrics, dict) or set(metrics) != {"value", "rows"}:
        raise ValueError("开发指标字段必须为value、rows")
    if isinstance(metrics["value"], bool) or not isinstance(metrics["value"], (float, int)):
        raise ValueError("开发指标值必须为数字")
    if type(metrics["rows"]) is not int or metrics["rows"] <= 0:
        raise ValueError("开发指标必须具有实际样本")
    if value.get("reflection") is not None:
        reflection = value["reflection"]
        if not isinstance(reflection, dict) or set(reflection) != _REFLECTION_FIELDS:
            raise ValueError("历史反思字段不完整")
    return value


def _context(value, *, reflection=False):
    if not isinstance(value, dict) or set(value) - _CONTEXT_FIELDS:
        raise ValueError("原生研究上下文字段无效")
    result = json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    _text(result.get("confirmed_spec"), "确认定义")
    fields = result.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(v, str) or not v for v in fields):
        raise ValueError("fields必须明确列出允许名称")
    if type(result.get("max_window")) is not int or result["max_window"] <= 0:
        raise ValueError("max_window必须为正整数")
    if not isinstance(result.get("expression_contract"), (str, dict)) or not result["expression_contract"]:
        raise ValueError("expression_contract必须明确表达式与开发指标合同")
    if "factor_contract" in result:
        from .factor_research import DAI_FACTOR_CONTRACT, factor_contract
        if factor_contract(result) == DAI_FACTOR_CONTRACT and fields != ["dai"]:
            raise ValueError("日级因子上下文只声明dai字段")
    history = result.get("history")
    if not isinstance(history, list):
        raise ValueError("history必须为开发实验列表")
    for record in history:
        _record(record)
    if "joint_knowledge" in result:
        from .joint_feedback import validate_joint_feedback
        validate_joint_feedback(result["joint_knowledge"])
    if "knowledge" in result:
        if not isinstance(result["knowledge"], list):
            raise ValueError("knowledge必须为有来源的开发知识列表")
        for record in result["knowledge"]:
            if not isinstance(record, dict) or set(record) - (_RECORD_FIELDS | {"record_id", "kind"}):
                raise ValueError("知识上下文只能包含开发事实")
            _text(record.get("record_id"), "知识引用")
            _record({k: v for k, v in record.items() if k not in {"record_id", "kind"}})
    if "direction" in result:
        from .direction_selection import validate_references
        validate_references(result["direction"]["knowledge_refs"], result.get("knowledge", []))
    if reflection:
        _record(result.get("current"))
        if result["current"].get("status", "evaluated") != "evaluated":
            raise ValueError("仅对已执行验证的实验生成研究反思")
    elif "current" in result:
        raise ValueError("提案上下文不得夹带未进入历史的当前结果")
    tokens = result.setdefault("max_output_tokens", 2048)
    if type(tokens) is not int or not 1 <= tokens <= 8192:
        raise ValueError("必须显式声明单次输出token预算")
    return result


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def _native_components():
    """只在明确调用研究组件时导入上游，不实例化其数据准备场景。"""
    from rdagent.components.proposal import FactorHypothesis2Experiment, FactorHypothesisGen
    from rdagent.core.proposal import Experiment2Feedback, Hypothesis, HypothesisFeedback, Trace
    from rdagent.core.scenario import Scenario
    from rdagent.oai.llm_utils import APIBackend
    from rdagent.utils.agent.tpl import T

    class DevelopmentScenario(Scenario):
        def __init__(self, context):
            self.context = context

        @property
        def background(self):
            return self.context["confirmed_spec"]

        @property
        def rich_style_description(self):
            return "正式开发范围内的因子研究"

        def get_runtime_environment(self):
            return "Qlib表达式；RP执行与独立验证；仅开发数据"

        def get_scenario_all_desc(self, task=None, filtered_tag=None, simple_background=None):
            facts = {key: self.context[key] for key in (
                "confirmed_spec", "fields", "max_window", "expression_contract",
            )}
            for key in ("development", "objective", "direction", "factor_contract"):
                if key in self.context:
                    facts[key] = self.context[key]
            return (
                _json(facts)
                + "\n每轮仅提出一个可检验的新因子。仅使用允许的字段和算子，不读取未来值。"
                + "因子组合方式以实验记录为准，不假定优胜因子已自动加入因子库。"
                + "metrics.rows是匹配目标表的指标行数，不是预测样本数；未提供预测样本数时不得据此推断样本规模。"
                + "Replace Best Result仅为研究建议，正式选模由RP按声明指标决定。"
            )

    class DevelopmentHypothesisGen(FactorHypothesisGen):
        def prepare_context(self, trace):
            context = {
                "hypothesis_output_format": T("scenarios.qlib.prompts:factor_hypothesis_output_format").r(),
                "hypothesis_specification": T("scenarios.qlib.prompts:factor_hypothesis_specification").r(),
            }
            history = trace.scen.context["history"]
            context["hypothesis_and_feedback"] = _json({**({"joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")} if "joint_knowledge" in trace.scen.context else {}), "history": history,
                "direction": trace.scen.context.get("direction")})
            context["last_hypothesis_and_feedback"] = _json({"history": history[-1:],
                "direction": trace.scen.context.get("direction")})
            context["RAG"] = _json({"knowledge": trace.scen.context.get("knowledge", []), "joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")}) if "joint_knowledge" in trace.scen.context else _json(trace.scen.context["knowledge"]) if "knowledge" in trace.scen.context else None
            if "direction" in trace.scen.context:
                context["direction"] = trace.scen.context["direction"]
            context["hypothesis_specification"] += "\n本轮只提出一个因子；不得复用历史中的同一表达式。"
            return context, True

        def convert_response(self, response):
            value = json.loads(response)
            for field in ("hypothesis", "reason"):
                _text(value.get(field), field)
            return Hypothesis(**{field: value.get(field) for field in _HYPOTHESIS_FIELDS})

    class DevelopmentHypothesis2Experiment(FactorHypothesis2Experiment):
        def prepare_context(self, hypothesis, trace):
            context = {
                "target_hypothesis": str(hypothesis),
                "hypothesis_and_feedback": _json({**({"joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")} if "joint_knowledge" in trace.scen.context else {}), "history": trace.scen.context["history"],
                    "direction": trace.scen.context.get("direction")}),
                "last_hypothesis_and_feedback": _json({"history": trace.scen.context["history"][-1:],
                    "direction": trace.scen.context.get("direction")}),
                "sota_hypothesis_and_feedback": "",
                "experiment_output_format": T("scenarios.qlib.prompts:factor_experiment_output_format").r(),
                "target_list": [], "RAG": _json({"knowledge": trace.scen.context.get("knowledge", []), "joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")}) if "joint_knowledge" in trace.scen.context else _json(trace.scen.context["knowledge"]) if "knowledge" in trace.scen.context else None,
            }
            if "direction" in trace.scen.context:
                context["direction"] = trace.scen.context["direction"]
            context["experiment_output_format"] += (
                "\n只返回一个因子。保留description、formulation和variables，"
                "另提供expression字段，其值为可执行的Qlib表达式字符串。"
            )
            if "direction" in trace.scen.context:
                context["experiment_output_format"] += (
                    "\n另提供knowledge_refs列表，引用本轮direction中实际采用的record_id；"
                    "表达式必须属于已选择的direction。"
                )
            return context, True

        def convert(self, hypothesis, trace):
            # 上游convert带无预算的重试；复用其模板和上下文，单次失败交回持久主循环。
            context, flag = self.prepare_context(hypothesis, trace)
            system = T("components.proposal.prompts:hypothesis2experiment.system_prompt").r(
                targets=self.targets, scenario=trace.scen.get_scenario_all_desc(),
                experiment_output_format=context["experiment_output_format"],
            )
            user = T("components.proposal.prompts:hypothesis2experiment.user_prompt").r(
                targets=self.targets, **context,
            )
            response = APIBackend().build_messages_and_create_chat_completion(
                user, system, json_mode=flag,
            )
            return self.convert_response(response, hypothesis, trace)

        def convert_response(self, response, hypothesis, trace):
            factors = json.loads(response)
            if not isinstance(factors, dict) or len(factors) != 1:
                raise ValueError("每轮实验必须定义一个因子")
            name, task = next(iter(factors.items()))
            _text(name, "因子名称")
            fields = {"description", "formulation", "variables", "expression"}
            if "direction" in trace.scen.context:
                fields.add("knowledge_refs")
            if not isinstance(task, dict) or set(task) != fields:
                raise ValueError("因子必须完整声明description、formulation、variables、expression")
            for field in ("description", "formulation", "expression"):
                _text(task[field], field)
            if not isinstance(task["variables"], dict) or any(
                not isinstance(k, str) or not isinstance(v, str) for k, v in task["variables"].items()
            ):
                raise ValueError("因子variables必须为名称到说明的映射")
            expression = "".join(task["expression"].split())
            prior = trace.scen.context["history"] + trace.scen.context.get("knowledge", [])
            if expression in {"".join(row.get("expression", "").split()) for row in prior}:
                raise ValueError("候选表达式已经研究过")
            result = {"expression": task["expression"], "hypothesis": hypothesis.hypothesis, "reason": hypothesis.reason}
            if "direction" in trace.scen.context:
                from .direction_selection import validate_references
                direction = trace.scen.context["direction"]
                validate_references(task["knowledge_refs"], trace.scen.context["knowledge"])
                if not set(task["knowledge_refs"]) <= set(direction["knowledge_refs"]):
                    raise ValueError("实验必须引用所选方向的知识")
                from .factor_research import expression_direction, factor_contract
                if expression_direction(task["expression"], contract=factor_contract(trace.scen.context)) != direction["direction"]:
                    raise ValueError("表达式与所选方向不一致")
                result["knowledge_refs"] = task["knowledge_refs"]
            return result

    class DevelopmentExperiment2Feedback(Experiment2Feedback):
        def generate_feedback(self, exp, trace):
            # 正式指标的名称和单位保持原样，不冒充上游固定IC或年化收益字段。
            system = T("scenarios.qlib.prompts:factor_feedback_generation.system").r(
                scenario=self.scen.get_scenario_all_desc(),
            )
            system += "\n实验组合以提供记录为准。负结果仍是有效研究，技术失败不能作为收益结论。"
            task = {
                "factor_name": exp.get("candidate_id", "current"),
                "factor_description": exp.get("reason", ""),
                "factor_formulation": exp.get("expression", ""),
                "variables": {}, "factor_implementation": "True",
            }
            user = T("scenarios.qlib.prompts:factor_feedback_generation.user").r(
                hypothesis_text=exp.get("hypothesis", ""), task_details=[task],
                combined_result=_json({"current": exp, "history": self.scen.context["history"], **({"joint_knowledge": self.scen.context["joint_knowledge"]} if "joint_knowledge" in self.scen.context else {})}),
            )
            response = APIBackend().build_messages_and_create_chat_completion(user, system, json_mode=True)
            value = json.loads(response)
            fields = {
                "observations": "Observations", "hypothesis_evaluation": "Feedback for Hypothesis",
                "new_hypothesis": "New Hypothesis", "reason": "Reasoning",
            }
            data = {key: _text(value.get(source), source) for key, source in fields.items()}
            decision = value.get("Replace Best Result")
            if type(decision) is bool:
                data["decision"] = decision
            elif isinstance(decision, str) and decision.lower() in {"yes", "no"}:
                data["decision"] = decision.lower() == "yes"
            else:
                raise ValueError("Replace Best Result必须为yes、no或布尔值")
            return HypothesisFeedback(**data)

    return DevelopmentScenario, Trace, DevelopmentHypothesisGen, DevelopmentHypothesis2Experiment, DevelopmentExperiment2Feedback


def propose_factor(context, complete):
    """调用上游因子假设流程和实验模板，返回纯JSON候选而不创建qrun工作区。"""
    data = _context(context)
    Scenario, Trace, HypothesisGen, Converter, _ = _native_components()
    scenario = Scenario(data)
    trace = Trace(scenario)
    with research_backend(complete, data["max_output_tokens"]):
        hypothesis = HypothesisGen(scenario).gen(trace)
        return Converter().convert(hypothesis, trace)


def reflect_factor(context, complete):
    """将已验证开发实验传给上游反思模板，返回可序列化的HypothesisFeedback。"""
    data = _context(context, reflection=True)
    Scenario, Trace, _, _, Summarizer = _native_components()
    scenario = Scenario(data)
    with research_backend(complete, data["max_output_tokens"]):
        feedback = Summarizer(scenario).generate_feedback(data["current"], Trace(scenario))
    return {field: getattr(feedback, field) for field in _REFLECTION_FIELDS}
