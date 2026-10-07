"""统一因子与模型的开发预算、方向和正式评价记录。"""
from copy import deepcopy
import json
from pathlib import Path
import re

from .contracts import write_json
from .factor_research import FactorResearch, ResearchBudgetExhausted, ResearchCalls, _freeze, _read, expression_direction, validate_factor_research, DAI_FACTOR_CONTRACT, DAI_EXPRESSION_CONTRACT, canonical_expression, factor_contract, factor_directions, feature_expression_slot
from .model_research import ModelResearch, validate_model_research
from .joint_feedback import validate_joint_feedback


def validate_joint_research(payload):
    fields = {"contract_version", "research_kind", "campaign_id", "session_root", "factor_request", "model_request", "budget", "proposer"}
    if not isinstance(payload, dict) or not fields <= set(payload) or set(payload) - fields - {"research_schedule"}:
        raise ValueError("联合研究请求字段不完整")
    value = json.loads(json.dumps(payload, allow_nan=False))
    if value["contract_version"] != "rd-joint-research-v1" or value["research_kind"] != "joint_research":
        raise ValueError("联合研究合同无效")
    if not isinstance(value["campaign_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value["campaign_id"]):
        raise ValueError("campaign_id必须为简单标识")
    if not isinstance(value["session_root"], str) or not value["session_root"]:
        raise ValueError("联合研究会话路径不能为空")
    factor = validate_factor_research(value["factor_request"])
    model = validate_model_research(value["model_request"])
    for key in ("source", "development", "objective"):
        if factor["package_template"][key] != model["package_template"][key]:
            raise ValueError("联合分支必须共用冻结输入、开发范围和指标")
    if "knowledge" in factor:
        raise ValueError("联合方向由统一调度选择，分支请求不接收独立方向知识索引")
    if factor["proposer"] != value["proposer"] or model["proposer"] != value["proposer"]:
        raise ValueError("联合分支必须共用提案器和响应序列")
    budget = value["budget"]
    if (not isinstance(budget, dict) or set(budget) != {"rounds", "evaluations", "model_calls", "output_tokens", "max_output_tokens_per_call"}
            or any(type(x) is not int or x < 1 for x in budget.values()) or budget["max_output_tokens_per_call"] > 8192):
        raise ValueError("联合研究预算无效")
    if "research_schedule" in value and (value["research_schedule"] != ["baseline", "factor", "model"] or budget["rounds"] != 3):
        raise ValueError("固定研究计划必须为三轮baseline、factor、model")
    return value


def select_joint_direction(context, complete):
    """使用固定上游动作模板，在已验证开发事实中选择分支。"""
    from .native_backend import research_backend
    from .direction_selection import validate_references
    from rdagent.oai.llm_utils import APIBackend
    from rdagent.utils.agent.tpl import T
    contract = factor_contract(context)
    scheduled = context.get("scheduled_action")
    if scheduled is not None and scheduled not in {"factor", "model"}:
        raise ValueError("本轮计划只能限定因子或模型分支")
    allowed = {*(("factor", direction) for direction in factor_directions(contract)),
               ("model", "generated_model"), ("stop", "stop")}
    if scheduled is not None:
        allowed = {choice for choice in allowed if choice[0] in {scheduled, "stop"}}
    records = validate_joint_feedback(context["knowledge"])
    if not records:
        return {"action": "stop", "direction": "stop", "knowledge_refs": [], "reason": "no_verified_development"}
    user = T("scenarios.qlib.prompts:action_gen.user").r(
        hypothesis_and_feedback=json.dumps(context, ensure_ascii=False),
        last_hypothesis_and_feedback=json.dumps(records[-1:], ensure_ascii=False))
    system = ("依据已验证开发知识选择因子或模型方向。只返回JSON，字段action、direction、knowledge_refs、reason。"
              "action为factor、model或stop；factor方向为momentum、mean_deviation、relative_volatility、range_position；"
              "model方向为generated_model；stop方向为stop。knowledge_refs引用实际采用的record_id，reason说明开发证据如何支持选择。"
              "因子仅使用close及1至5期窗口，模型仅使用受支持的前馈有向无环网络。"
              "模型消费已验证因子中开发指标最好的表达式，标签、处理器与训练设置固定。"
              "不可扩展数据、预算或使用最终test/holdout。不同分支的负结果也应保留。")
    if contract == DAI_FACTOR_CONTRACT:
        system = ("依据已验证开发知识选择已确认日级因子或模型方向。只返回JSON，字段action、direction、knowledge_refs、reason。"
                  "action为factor、model或stop；factor方向只允许lag或mean，model方向为generated_model，stop方向为stop。"
                  + DAI_EXPRESSION_CONTRACT +
                  "模型消费开发指标最好的已验证因子，写入dai_following槽；仅改变受支持的前馈网络结构。"
                  "knowledge_refs引用实际采用的record_id；特征资格、标签、处理器、切分与训练设置冻结，不使用test/holdout或扩大预算。")
    if scheduled is not None:
        system += f"本轮冻结计划限定{scheduled}分支，action只能为{scheduled}或stop，direction必须属于该分支；不得返回另一分支。"
    with research_backend(complete, context["max_output_tokens"]):
        result = json.loads(APIBackend().build_messages_and_create_chat_completion(user, system, json_mode=True))
    if (not isinstance(result, dict) or set(result) != {"action", "direction", "knowledge_refs", "reason"}
            or (result["action"], result["direction"]) not in allowed
            or not isinstance(result["reason"], str) or not result["reason"].strip()):
        raise ValueError("联合方向必须是受支持的因子、模型或停止")
    validate_references(result["knowledge_refs"], records)
    return result


class JointFactorResearch(FactorResearch):
    def context(self):
        result = super().context()
        if hasattr(self, "call_owner"):
            result.update(joint_knowledge=self.call_owner.knowledge_view(), joint_direction=self.call_owner.active_direction)
        return result


class JointModelResearch(ModelResearch):
    def context(self):
        result = super().context()
        if factor_contract(self.design) == DAI_FACTOR_CONTRACT:
            result.update(factor_contract=DAI_FACTOR_CONTRACT, factor_feature_slot="dai_following")
        if hasattr(self, "call_owner"):
            result.update(joint_knowledge=self.call_owner.knowledge_view(), joint_direction=self.call_owner.active_direction)
        return result

    def _candidate_payload(self, proposal):
        from research_pipeline.platform import typed_canonical_hash
        payload = super()._candidate_payload(proposal)
        overrides = payload["candidates"][0]["parameter_overrides"]
        design = deepcopy(next(item["value"] for item in overrides if item["node_id"] == "feature" and item["parameter_name"] == "design"))
        design["feature_expressions"] = {feature_expression_slot(design): canonical_expression(proposal["factor_expression"], contract=factor_contract(design))}
        identity = typed_canonical_hash(design)
        for item in overrides:
            if item["parameter_name"] == "design":
                item["value"] = design
            elif item["parameter_name"] in {"lineage_ref", "research_identity_hash"}:
                item["value"] = identity
        return payload


class JointResearch:
    def __init__(self, payload, *, model_env_file=None):
        self.payload = validate_joint_research(payload)
        self.root = Path(self.payload["session_root"]).resolve()
        self.model_env_file = model_env_file
        self.active_direction = None
        from research_pipeline.data_plane import PathRolePolicy
        source = self.payload["factor_request"]["package_template"]["source"]
        paths = {key: source[key] for key in ("path", "catalog_lock", "verifier_bundle", "input_snapshot_manifest", "source_archive_root")}
        paths.update({"extension_" + str(i): value for i, value in enumerate(source["extension_bundles"])})
        paths.update({"confirmation_" + key: value for key, value in self.payload["factor_request"]["confirmed_spec"].items() if key != "kind"})
        paths["model_definition"] = self.payload["model_request"]["confirmed_spec"]
        paths.update({"response_" + str(i): value for i, value in enumerate(self.payload["proposer"].get("responses", []))})
        paths.update({"coding_source_" + str(i): value for i, value in enumerate(self.payload["model_request"].get("coding_knowledge", {}).get("source_sessions", []))})
        PathRolePolicy().validate({"output": str(self.root), **paths}, read_only_roles=tuple(paths))
        _freeze(self.root / "request.json", self.payload)
        self.materials = {"responses": [Path(p).read_text(encoding="utf-8") for p in self.payload["proposer"].get("responses", [])]}
        _freeze(self.root / "materials.json", self.materials)
        self.branches = {}
        for kind, cls in (("factor", JointFactorResearch), ("model", JointModelResearch)):
            request = deepcopy(self.payload[kind + "_request"])
            request["campaign_id"] = self.payload["campaign_id"] + "-" + kind
            request["session_root"] = str(self.root / "branches" / kind)
            request["budget"].update({k: v for k, v in self.payload["budget"].items() if k != "evaluations"})
            branch = cls(request, model_env_file=model_env_file)
            branch.call_owner, branch.call_prefix = self, kind + "-"
            self.branches[kind] = branch
        factor, model = self.branches["factor"], self.branches["model"]
        if factor.design != model.design or factor.metric_description != model.metric_description:
            raise ValueError("联合分支必须共用同一正式设计与指标定义")
        slot = feature_expression_slot(factor.design)
        if set(factor.design.get("feature_expressions", {})) != {slot}:
            raise ValueError("联合模型必须绑定当前合同的唯一表达式槽：" + slot)
        history = self.history()
        if history and history[0]["status"] == "evaluated" and not (self.root / "development-population.json").exists():
            self._check_population(0, history[0])

    def history(self):
        return [_read(p) for p in sorted((self.root / "rounds").glob("*/record.json"))]

    def knowledge_view(self):
        records = []
        fields = ("record_id", "kind", "candidate_id", "hypothesis", "reason", "expression", "definition", "status", "reflection")
        for row in self.history():
            if row["status"] != "evaluated":
                continue
            if row["metrics"].get("verification_status") != "pass":
                raise ValueError("联合反馈缺少通过的独立验证")
            item = {key: row[key] for key in fields if key in row}
            item["metrics"] = {key: row["metrics"][key] for key in ("value", "rows")}
            records.append(item)
        return validate_joint_feedback(records)

    def propose(self, index):
        path = self.root / "rounds" / f"{index:04d}" / "proposal.json"
        if path.exists():
            result = _read(path)
            self.active_direction = result["direction_choice"]
            return result
        direction_path = path.with_name("direction.json")
        if direction_path.exists():
            direction = _read(direction_path)
        elif index == 0:
            direction = {"action": "factor", "direction": expression_direction(self.payload["factor_request"]["baseline"]["expression"], contract=factor_contract(self.payload["factor_request"])),
                         "knowledge_refs": [], "reason": "confirmed_document_baseline"}
            _freeze(direction_path, direction)
        else:
            try:
                context = {"knowledge": self.knowledge_view(),
                    "development": self.payload["factor_request"]["package_template"]["development"],
                    "objective": {**self.payload["factor_request"]["package_template"]["objective"], "metric_definition": self.branches["factor"].metric_description},
                    "max_output_tokens": self.payload["budget"]["max_output_tokens_per_call"]}
                if factor_contract(self.payload["factor_request"]) == DAI_FACTOR_CONTRACT:
                    context["factor_contract"] = DAI_FACTOR_CONTRACT
                if "research_schedule" in self.payload:
                    context["scheduled_action"] = self.payload["research_schedule"][index]
                direction = select_joint_direction(context, ResearchCalls(self, f"{index:04d}-direction"))
            except ResearchBudgetExhausted:
                direction = {"action": "stop", "direction": "stop", "knowledge_refs": [], "reason": "model_budget"}
            except ValueError:
                direction = {"action": "stop", "direction": "stop", "knowledge_refs": [], "reason": "direction_invalid"}
            _freeze(direction_path, direction)
        self.active_direction = direction
        kind = direction["action"]
        if kind == "stop":
            result = {"action": "stop", "status": "stopped", "reason": direction["reason"]}
        elif len(list((self.root / "rounds").glob("*/evaluation-reservation.json"))) >= self.payload["budget"]["evaluations"]:
            result = {"action": "stop", "status": "stopped", "reason": "evaluation_budget"}
        else:
            result = dict(self.branches[kind].propose(index))
            if kind == "factor" and result.get("action") not in {"stop", "reject"} and expression_direction(result["expression"], contract=factor_contract(self.payload["factor_request"])) != direction["direction"]:
                result = {"action": "reject", "reason": "factor_direction_mismatch"}
            if kind == "model" and result.get("action") not in {"stop", "reject"}:
                candidates = [row for row in self.history() if row.get("kind") == "factor" and row["status"] == "evaluated"]
                sign = 1 if self.payload["factor_request"]["package_template"]["objective"]["direction"] == "minimize" else -1
                factor = min(candidates, key=lambda row: sign * row["metrics"]["value"])
                result.update(factor_record_id=factor["record_id"], factor_expression=factor["expression"])
        result.update(round=index, kind=kind, direction_choice=direction, knowledge_refs=direction["knowledge_refs"])
        _freeze(path, result)
        return result

    def _check_population(self, index, result):
        from research_pipeline.evidence import load_verified_result_context
        from .joint_feedback import development_population
        reference = self.branches[result["kind"]].root / "experiments" / result["candidate_id"] / "candidates" / result["candidate_id"] / "execution-ref.json"
        execution = Path(_read(reference)["execution_root"])
        context = load_verified_result_context(result["metrics"]["verification_ref"], result_store=execution / "results")
        bundle = context.snapshot.bundle
        facts = json.loads(context.snapshot.support_bytes[bundle.verification.validity_source_path])
        population = development_population(facts["model_diagnostics"])
        path = self.root / "development-population.json"
        if index == 0:
            _freeze(path, population)
        elif _read(path) != population:
            raise ValueError("联合候选与基线的样本或validation标签不同")
        _freeze(self.root / "rounds" / f"{index:04d}" / "population-check.json", {
            "status": "pass", "sample_count": len(population["samples"]), "validation_sample_count": len(population["validation"])})

    def evaluate(self, index, proposal):
        path = self.root / "rounds" / f"{index:04d}" / "evaluation.json"
        if path.exists():
            return _read(path)
        if proposal.get("action") in {"stop", "reject"}:
            result = {**proposal, "status": "stopped" if proposal["action"] == "stop" else "rejected"}
        else:
            reservation = path.with_name("evaluation-reservation.json")
            if not reservation.exists() and len(list((self.root / "rounds").glob("*/evaluation-reservation.json"))) >= self.payload["budget"]["evaluations"]:
                result = {**proposal, "status": "stopped", "reason": "evaluation_budget"}
                _freeze(path, result)
                return result
            _freeze(reservation, {"round": index, "kind": proposal["kind"], "candidate_id": proposal["candidate_id"]})
            try:
                result = self.branches[proposal["kind"]].evaluate(index, proposal)
            except ValueError:
                result = {**proposal, "status": "failed", "reason": "formal_verification_or_metric_failed"}
            if result["status"] == "evaluated":
                try:
                    self._check_population(index, result)
                except ValueError:
                    result = {key: value for key, value in result.items() if key != "metrics"}
                    result.update(status="failed", reason="development_population_mismatch")
        _freeze(path, result)
        return result

    def reflect(self, index, evaluation):
        path = self.root / "rounds" / f"{index:04d}" / "reflection.json"
        if path.exists():
            return _read(path)
        if evaluation["status"] == "evaluated":
            self.active_direction = evaluation["direction_choice"]
            result = self.branches[evaluation["kind"]].reflect(index, evaluation)
        else:
            result = evaluation
        _freeze(path, result)
        return result

    def record(self, index, value):
        result = {**value, "record_id": self.payload["campaign_id"] + f":{index:04d}"}
        _freeze(self.root / "rounds" / f"{index:04d}" / "record.json", result)
        if value["kind"] in self.branches:
            branch = self.branches[value["kind"]]
            _freeze(branch.root / "rounds" / f"{index:04d}" / "record.json", value)
        if (index + 1 >= self.payload["budget"]["rounds"] or value["status"] == "stopped"
                or index == 0 and value["status"] != "evaluated"
                or value.get("reflection_status") in {"budget_exhausted", "invalid_response"}):
            self.finish()
        return result

    def finish(self):
        history = self.history()
        complete = (len(history) == self.payload["budget"]["rounds"] and all(row["status"] == "evaluated" and "reflection" in row for row in history))
        kinds = sorted({row["kind"] for row in history if row["status"] == "evaluated"})
        calls = [_read(p) for p in (self.root / "calls").glob("*.json")]
        result = {"status": "completed" if complete else "incomplete", "scope": "development_joint_research",
            "campaign_id": self.payload["campaign_id"], "rounds": history, "executed_branches": kinds,
            "holdout_evaluated": False, "paid_model_calls": sum(c["transport"] == "live" for c in calls),
            "reserved_evaluations": len(list((self.root / "rounds").glob("*/evaluation-reservation.json"))), "reserved_model_calls": len(calls), "reserved_output_tokens": sum(c["request"]["max_output_tokens"] for c in calls)}
        _freeze(self.root / "outcome.json", result)
        return result
