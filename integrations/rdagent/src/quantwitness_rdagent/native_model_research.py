"""上游模型假设、网络结构编译及开发反思适配。"""
import json
import math

from .native_backend import research_backend
from .native_research import _HYPOTHESIS_FIELDS, _REFLECTION_FIELDS, _text

MODEL_CONTRACT = (
    "输入为已冻结处理器产生的表格特征，输出一个原始收益回归值。"
    "只生成前馈有向无环图definition.nodes，共1至8节点。"
    "每节点含inputs（-1为原输入，其余为此前节点编号）、width（1至64）、"
    "activation（identity/relu/tanh/gelu）。多输入按列拼接；末节点width=1、activation=identity。"
    "所有节点必须参与输出；不得修改特征、标签、分段、处理器或训练超参数。"
)


def _context(value, reflection=False):
    fields = {"confirmed_spec", "development", "objective", "history", "max_output_tokens", "current", "joint_knowledge", "joint_direction", "factor_contract", "factor_feature_slot"}
    if not isinstance(value, dict) or set(value) - fields:
        raise ValueError("模型研究只能使用已验证开发上下文")
    data = json.loads(json.dumps(value, allow_nan=False))
    _text(data.get("confirmed_spec"), "研究定义")
    if not isinstance(data.get("history"), list):
        raise ValueError("模型研究缺少历史")
    if "joint_knowledge" in data:
        from .joint_feedback import validate_joint_feedback
        validate_joint_feedback(data["joint_knowledge"])
    if "factor_contract" in data:
        from .factor_research import DAI_FACTOR_CONTRACT, factor_contract
        if factor_contract(data) != DAI_FACTOR_CONTRACT or data.get("factor_feature_slot") != "dai_following":
            raise ValueError("日级联合模型必须绑定dai_following槽")
    records = data["history"] + ([data.get("current")] if reflection else [])
    if not reflection and "current" in data:
        raise ValueError("模型提案不能夹带当前结果")
    for row in records:
        if not isinstance(row, dict) or set(row) - {"candidate_id", "hypothesis", "reason", "definition", "status", "metrics", "reflection"}:
            raise ValueError("模型研究记录字段不受支持")
        if row.get("status") == "evaluated":
            metric = row.get("metrics")
            if (not isinstance(metric, dict) or set(metric) != {"value", "rows"}
                    or type(metric["value"]) not in (int, float) or not math.isfinite(metric["value"])
                    or type(metric["rows"]) is not int or metric["rows"] < 1):
                raise ValueError("模型开发指标不完整")
        elif set(row) & {"metrics", "reflection"}:
            raise ValueError("技术失败不能携带金融结论")
    if reflection and data["current"].get("status") != "evaluated":
        raise ValueError("只能反思通过正式验证的开发结果")
    if type(data.get("max_output_tokens")) is not int or not 1 <= data["max_output_tokens"] <= 8192:
        raise ValueError("单次模型输出预算无效")
    return data


def _components():
    from rdagent.components.proposal import ModelHypothesisGen, ModelHypothesis2Experiment
    from rdagent.core.proposal import Experiment2Feedback, Hypothesis, HypothesisFeedback, Trace
    from rdagent.core.scenario import Scenario
    from rdagent.oai.llm_utils import APIBackend
    from rdagent.utils.agent.tpl import T

    class ModelScenario(Scenario):
        def __init__(self, context):
            self.context = context

        @property
        def background(self):
            return self.context["confirmed_spec"]

        @property
        def rich_style_description(self):
            return "正式开发区的生成式前馈模型研究"

        def get_runtime_environment(self):
            return "Qlib 0.9.7 / Torch 2.5.1 CPU；正式Workspace与独立验证"

        def get_scenario_all_desc(self, **kwargs):
            facts = {k: self.context[k] for k in ("confirmed_spec", "development", "objective")}
            if "factor_contract" in self.context:
                facts.update(factor_contract=self.context["factor_contract"], factor_feature_slot=self.context["factor_feature_slot"])
            return json.dumps(facts, ensure_ascii=False) + MODEL_CONTRACT

    class Generator(ModelHypothesisGen):
        def prepare_context(self, trace):
            history = json.dumps({"history": trace.scen.context["history"], "joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")} if "joint_knowledge" in trace.scen.context else trace.scen.context["history"], ensure_ascii=False)
            return {"hypothesis_output_format": '{"hypothesis":"新模型假设", "reason":"依据"}',
                    "hypothesis_specification": T("scenarios.qlib.prompts:model_hypothesis_specification").r() + MODEL_CONTRACT,
                    "hypothesis_and_feedback": history, "last_hypothesis_and_feedback": history,
                    "RAG": json.dumps({"knowledge": trace.scen.context.get("joint_knowledge", []), "direction": trace.scen.context.get("joint_direction")}, ensure_ascii=False) if "joint_knowledge" in trace.scen.context else None}, True

        def convert_response(self, response):
            data = json.loads(response)
            for key in ("hypothesis", "reason"):
                _text(data.get(key), key)
            return Hypothesis(**{key: data.get(key) for key in _HYPOTHESIS_FIELDS})

    class Converter(ModelHypothesis2Experiment):
        def prepare_context(self, hypothesis, trace):
            return {"target_hypothesis": str(hypothesis),
                    "hypothesis_and_feedback": json.dumps({"history": trace.scen.context["history"], "joint_knowledge": trace.scen.context["joint_knowledge"], "joint_direction": trace.scen.context.get("joint_direction")} if "joint_knowledge" in trace.scen.context else trace.scen.context["history"], ensure_ascii=False),
                    "experiment_output_format": '只返回JSON对象{"definition":{"nodes":[...]}}。' + MODEL_CONTRACT,
                    "target_list": [], "RAG": json.dumps(trace.scen.context["joint_knowledge"], ensure_ascii=False) if "joint_knowledge" in trace.scen.context else None, "last_hypothesis_and_feedback": "", "sota_hypothesis_and_feedback": ""}, True

        def convert_response(self, response, hypothesis, trace):
            return response

        def convert(self, hypothesis, trace):
            # 使用上游模型模板，失败由有预算且持久化的修复流程处理。
            context, flag = self.prepare_context(hypothesis, trace)
            system = T("components.proposal.prompts:hypothesis2experiment.system_prompt").r(
                targets=self.targets, scenario=trace.scen.get_scenario_all_desc(),
                experiment_output_format=context["experiment_output_format"])
            user = T("components.proposal.prompts:hypothesis2experiment.user_prompt").r(targets=self.targets, **context)
            response = APIBackend().build_messages_and_create_chat_completion(user, system, json_mode=flag)
            return self.convert_response(response, hypothesis, trace)

    class Feedback(Experiment2Feedback):
        def generate_feedback(self, exp, trace):
            system = T("scenarios.qlib.prompts:model_feedback_generation.system").r(scenario=self.scen.get_scenario_all_desc())
            system += "\n仅按提供的开发指标及单位判断；未提供回测与训练日志。rows是指标行数，不是预测样本量。Decision仅为建议。"
            response = APIBackend().build_messages_and_create_chat_completion(
                json.dumps({"current": exp, "history": self.scen.context["history"], **({"joint_knowledge": self.scen.context["joint_knowledge"]} if "joint_knowledge" in self.scen.context else {})}, ensure_ascii=False), system, json_mode=True)
            value = json.loads(response)
            mapping = {"observations": "Observations", "hypothesis_evaluation": "Feedback for Hypothesis",
                       "new_hypothesis": "New Hypothesis", "reason": "Reasoning"}
            data = {key: _text(value.get(source), source) for key, source in mapping.items()}
            if type(value.get("Decision")) is not bool:
                raise ValueError("模型反馈Decision必须为布尔值")
            return HypothesisFeedback(**data, decision=value["Decision"])

    return ModelScenario, Trace, Generator, Converter, Feedback


def propose_model(context, complete):
    """保留原始结构响应，交给持久化编译与修复步骤验收。"""
    data = _context(context)
    Scenario, Trace, Generator, Converter, _ = _components()
    scenario = Scenario(data)
    trace = Trace(scenario)
    with research_backend(complete, data["max_output_tokens"]):
        hypothesis = Generator(scenario).gen(trace)
        response = Converter().convert(hypothesis, trace)
    return {"hypothesis": hypothesis.hypothesis, "reason": hypothesis.reason, "response": response}


def reflect_model(context, complete):
    data = _context(context, reflection=True)
    Scenario, Trace, _, _, Feedback = _components()
    scenario = Scenario(data)
    with research_backend(complete, data["max_output_tokens"]):
        feedback = Feedback(scenario).generate_feedback(data["current"], Trace(scenario))
    return {key: getattr(feedback, key) for key in _REFLECTION_FIELDS}
