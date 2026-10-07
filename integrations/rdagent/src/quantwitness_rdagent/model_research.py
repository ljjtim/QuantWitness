"""运行时模型结构研究，逐轮委托正式开发研究包并封存编译经验。"""
from copy import deepcopy
import json
from pathlib import Path
import re

from .contracts import write_json
from .factor_research import ResearchCalls, ResearchBudgetExhausted, _freeze, _read
from .package_campaign import PackageCampaign, validate_package_campaign


def validate_model_research(payload):
    fields = {"contract_version", "research_kind", "campaign_id", "session_root", "package_template",
              "confirmed_spec", "baseline", "training", "budget", "proposer"}
    if not isinstance(payload, dict) or not fields <= set(payload) or set(payload) - fields - {"coding_knowledge"}:
        raise ValueError("模型研究请求字段不完整")
    value = json.loads(json.dumps(payload, allow_nan=False))
    if value["contract_version"] != "rd-model-research-v1" or value["research_kind"] != "model_research":
        raise ValueError("模型研究合同无效")
    if not isinstance(value["campaign_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value["campaign_id"]):
        raise ValueError("campaign_id必须为简单标识")
    for key in ("session_root", "confirmed_spec"):
        if not isinstance(value[key], str) or not value[key]:
            raise ValueError("模型研究路径不能为空")
    template = validate_package_campaign(value["package_template"])
    if template["proposer"] != {"mode": "fixed_policy"} or len(template["candidates"]) != 1:
        raise ValueError("模型执行模板必须只含一个固定基准")
    baseline = value["baseline"]
    if not isinstance(baseline, dict) or set(baseline) != {"definition", "hypothesis", "reason"}:
        raise ValueError("模型基准必须完整声明定义和依据")
    from research_pipeline.research.modeling.generated_definition import validate_kwargs
    validate_kwargs({"definition": baseline["definition"], **value["training"]})
    if set(value["training"]) != {"epochs", "learning_rate", "early_stop", "l2"}:
        raise ValueError("模型训练设置必须固定")
    if any(not isinstance(baseline[k], str) or not baseline[k].strip() for k in ("hypothesis", "reason")):
        raise ValueError("基准假设与依据不能为空")
    budget = value["budget"]
    if (not isinstance(budget, dict) or set(budget) != {"rounds", "model_calls", "output_tokens", "max_output_tokens_per_call", "repairs"}
            or any(type(v) is not int or v < (0 if k == "repairs" else 1) for k, v in budget.items())
            or budget["max_output_tokens_per_call"] > 8192):
        raise ValueError("模型研究预算无效")
    proposer = value["proposer"]
    if proposer.get("mode") == "fixed_responses":
        if set(proposer) != {"mode", "responses"} or not isinstance(proposer["responses"], list) or any(not isinstance(x, str) or not x for x in proposer["responses"]):
            raise ValueError("固定响应路径无效")
    elif proposer.get("mode") == "live":
        from urllib.parse import urlsplit
        if set(proposer) != {"mode", "model", "base_url"} or not isinstance(proposer["model"], str) or not proposer["model"]:
            raise ValueError("live模型身份不完整")
        endpoint = urlsplit(proposer["base_url"])
        if endpoint.scheme != "https" or not endpoint.netloc or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            raise ValueError("live模型地址无效")
    else:
        raise ValueError("模型提案只支持固定响应或live")
    if "coding_knowledge" in value:
        knowledge = value["coding_knowledge"]
        if not isinstance(knowledge, dict) or set(knowledge) != {"source_sessions", "model_family"} or not isinstance(knowledge["source_sessions"], list):
            raise ValueError("模型编码经验必须声明来源会话与模型族")
    return value


class ModelResearch:
    def __init__(self, payload, *, model_env_file=None):
        self.payload = validate_model_research(payload)
        self.root = Path(self.payload["session_root"]).resolve()
        self.model_env_file = model_env_file
        live = self.payload["proposer"]["mode"] == "live"
        if live != bool(model_env_file):
            raise ValueError("live必须显式提供.env，固定响应不接收凭据")
        if live:
            from .model_client import public_config
            if public_config(model_env_file) != {k: self.payload["proposer"][k] for k in ("model", "base_url")}:
                raise ValueError(".env模型身份与冻结请求不同")
        from research_pipeline.packages import load_research_package
        from research_pipeline.data_plane import PathRolePolicy
        from research_pipeline.extensions.verifier_bundle import verify_project_verifier_bundle
        source = self.payload["package_template"]["source"]
        paths = {k: source[k] for k in ("path", "catalog_lock", "verifier_bundle", "input_snapshot_manifest", "source_archive_root")}
        paths.update({"extension_"+str(i): p for i, p in enumerate(source["extension_bundles"])})
        paths["confirmation"] = self.payload["confirmed_spec"]
        paths.update({"response_"+str(i): p for i, p in enumerate(self.payload["proposer"].get("responses", []))})
        paths.update({"knowledge_"+str(i): p for i, p in enumerate(self.payload.get("coding_knowledge", {}).get("source_sessions", []))})
        PathRolePolicy().validate({"output": str(self.root), **paths}, read_only_roles=tuple(paths))
        package = load_research_package(source["path"])
        nodes = {n["node_id"]: n for n in package.spec_payload["graph"]["nodes"]}
        if not {"feature", "label", "summary", "model_fit"} <= set(nodes):
            raise ValueError("模型研究需要日频Qlib研究包")
        self.design = json.loads(json.dumps(nodes["feature"]["parameters"]["design"], default=dict))
        if self.design.get("mode") != "development" or self.design.get("sequence"):
            raise ValueError("生成模型研究只接受表格development包")
        self.fit_parameters = dict(nodes["model_fit"]["parameters"])
        self.processors = json.loads(self.fit_parameters["candidate_jsons"][0])["processors"]
        verifier = verify_project_verifier_bundle(source["verifier_bundle"])
        objective = self.payload["package_template"]["objective"]
        definitions = [d for d in verifier.metric_definitions if d.metric_ref == self.design["metric_ref"] and d.result_schema_id == objective["schema_id"]]
        if len(definitions) != 1:
            raise ValueError("模型开发目标缺少唯一正式指标定义")
        definition = definitions[0].payload()
        if (objective["direction"] == "minimize") != (definition["direction"] == "lower_is_better"):
            raise ValueError("模型开发目标方向与正式指标不同")
        self.metric_description = {k: definition[k] for k in ("metric_id", "unit", "frequency", "direction", "annualization_policy", "measurement_semantics")}
        self.materials = {"text": Path(self.payload["confirmed_spec"]).read_text(encoding="utf-8"),
                          "responses": [Path(p).read_text(encoding="utf-8") for p in self.payload["proposer"].get("responses", [])]}
        if not self.materials["text"].strip():
            raise ValueError("模型研究定义不能为空")
        _freeze(self.root / "request.json", self.payload)
        _freeze(self.root / "materials.json", self.materials)
        template = deepcopy(self.payload["package_template"])
        template["session_root"] = str(self.root / "frozen-template")
        self.template = PackageCampaign(template)
        self.knowledge = None
        if "coding_knowledge" in self.payload:
            from .model_knowledge import ModelCodingKnowledge
            from .model_research_evidence import validate_coding_source
            from research_pipeline.research.modeling.generated_definition import compile_source
            knowledge = self.payload["coding_knowledge"]
            self.knowledge = ModelCodingKnowledge(self.root, task={"interface": "generated-dag-v1", "model_family": knowledge["model_family"]},
                source_sessions=knowledge["source_sessions"], validate_source=validate_coding_source,
                source_template=compile_source(self.payload["baseline"]["definition"], 1))

    def history(self):
        return [_read(p) for p in sorted((self.root / "rounds").glob("*/record.json"))]

    def context(self):
        history = []
        for row in self.history():
            item = {k: row[k] for k in ("candidate_id", "hypothesis", "reason", "definition", "status") if k in row}
            if row["status"] == "evaluated":
                item["metrics"] = {k: row["metrics"][k] for k in ("value", "rows")}
                if "reflection" in row:
                    item["reflection"] = row["reflection"]
            history.append(item)
        template = self.payload["package_template"]
        return {"confirmed_spec": self.materials["text"], "development": template["development"],
                "objective": {**template["objective"], "metric_definition": self.metric_description},
                "history": history, "max_output_tokens": self.payload["budget"]["max_output_tokens_per_call"]}

    def propose(self, index):
        path = self.root / "rounds" / f"{index:04d}" / "proposal.json"
        if path.exists():
            return _read(path)
        if index == 0:
            proposal = deepcopy(self.payload["baseline"])
        else:
            from .native_model_research import propose_model, MODEL_CONTRACT
            from research_pipeline.research.modeling.generated_definition import validate_definition, compile_source
            from research_pipeline.research.modeling.inputs import QlibModelError
            raw_path = path.with_name("raw-proposal.json")
            try:
                if raw_path.exists():
                    raw = _read(raw_path)
                else:
                    raw = propose_model(self.context(), ResearchCalls(self, f"{index:04d}-proposal"))
                    _freeze(raw_path, raw)
                response = raw["response"]
                for attempt in range(self.payload["budget"]["repairs"] + 1):
                    attempt_path = path.parent / f"compile-{attempt:02d}.json"
                    try:
                        value = json.loads(response)
                        if not isinstance(value, dict) or set(value) != {"definition"}:
                            raise ValueError("模型响应只能包含definition")
                        definition = validate_definition(value["definition"])
                        if any(row.get("definition") == definition for row in self.history()):
                            raise ValueError("模型结构与已有实验重复")
                        source = compile_source(definition, 1)
                        _freeze(attempt_path, {"status": "pass", "response": response, "source": source})
                        proposal = {"definition": definition, "hypothesis": raw["hypothesis"], "reason": raw["reason"]}
                        break
                    except (ValueError, QlibModelError, TypeError, KeyError) as exc:
                        error = str(exc)
                        _freeze(attempt_path, {"status": "fail", "response": response, "error": error})
                        if self.knowledge:
                            self.knowledge.record(candidate_id=f"model_{index:04d}_attempt_{attempt}", source=response,
                                evidence={"model_status": "fail", "diagnostics": [{"stage": "model_validation", "error_code": "model.definition_invalid", "error_type": "ValueError"}]})
                        if attempt >= self.payload["budget"]["repairs"]:
                            proposal = {"action": "reject", "reason": "model_compile_failed"}
                            break
                        knowledge = self.knowledge.query(index * (self.payload["budget"]["repairs"] + 1) + attempt) if self.knowledge else []
                        prompt = json.dumps({"hypothesis": raw["hypothesis"], "response": response, "technical_error": error, "coding_knowledge": knowledge}, ensure_ascii=False)
                        response = ResearchCalls(self, f"{index:04d}-repair-{attempt}")(prompt, instructions=MODEL_CONTRACT + '返回{"definition":{"nodes":[...]}}。')
            except ResearchBudgetExhausted:
                proposal = {"action": "stop", "reason": "model_budget"}
            except (ValueError, QlibModelError) as exc:
                proposal = {"action": "reject", "reason": "invalid_model_response", "error_type": type(exc).__name__}
        proposal.update(candidate_id=f"model_{index:04d}", round=index)
        _freeze(path, proposal)
        return proposal

    def _candidate_payload(self, proposal):
        from datetime import datetime
        from research_pipeline.platform import canonical_json, typed_canonical_hash
        from research_pipeline.research.validation import build_search_manifest
        from research_pipeline.research.modeling.walk_forward import normalize_model_candidates
        candidate = {"candidate_id": proposal["candidate_id"], "model": {"class": "GeneratedModel", "module_path": "research_pipeline.research.modeling.generated",
                     "kwargs": {"definition": proposal["definition"], **self.payload["training"]}}, "processors": self.processors, "fit": {}}
        candidates = normalize_model_candidates([candidate])
        search = build_search_manifest(search_id=self.fit_parameters["search_id"], candidates=candidates, method="grid", max_trials=1, max_parallel=1,
                    stopping_condition="complete_declared_candidate_universe", objective="neg_mean_squared_error", direction="maximize",
                    frozen_at=datetime.fromisoformat(self.fit_parameters["search_frozen_at"]))
        design = deepcopy(self.design)
        design["candidate_ids"] = [item.candidate_id for item in search.candidates]
        design["candidate_parameters_json"] = canonical_json({item.candidate_id: dict(item.parameters) for item in search.candidates})
        identity = typed_canonical_hash(design)
        overrides = [{"node_id": "model_fit", "parameter_name": "candidate_jsons", "value": [canonical_json(c) for c in candidates]},
                     {"node_id": "model_fit", "parameter_name": "research_identity_hash", "value": identity}]
        for name in ("feature", "label", "summary"):
            overrides.append({"node_id": name, "parameter_name": "design", "value": design})
            if name != "summary":
                overrides.append({"node_id": name, "parameter_name": "lineage_ref", "value": identity})
        payload = deepcopy(self.payload["package_template"])
        payload.update(campaign_id=self.payload["campaign_id"]+"-"+proposal["candidate_id"],
                       session_root=str(self.root / "experiments" / proposal["candidate_id"]),
                       candidates=[{"id": proposal["candidate_id"], "parameter_overrides": overrides}], baseline_id=proposal["candidate_id"])
        return payload

    def evaluate(self, index, proposal):
        path = self.root / "rounds" / f"{index:04d}" / "evaluation.json"
        if path.exists():
            return _read(path)
        if self.template.prepare_data() != self.template.data:
            raise ValueError("模型基包或输入已改变")
        result = dict(proposal)
        if proposal.get("action") in {"stop", "reject"}:
            result["status"] = "stopped" if proposal["action"] == "stop" else "rejected"
        else:
            from .model_execution import evaluate_model_package
            result["metrics"] = evaluate_model_package(self._candidate_payload(proposal))
            result["status"] = "evaluated"
            if self.knowledge:
                from .model_research_evidence import record_coding_success
                record_coding_success(self, proposal, result)
        _freeze(path, result)
        return result

    def reflect(self, index, evaluation):
        path = self.root / "rounds" / f"{index:04d}" / "reflection.json"
        if path.exists():
            return _read(path)
        result = dict(evaluation)
        if result["status"] == "evaluated":
            from .native_model_research import reflect_model
            context = self.context()
            context["current"] = {k: evaluation[k] for k in ("candidate_id", "definition", "hypothesis", "reason", "status")}
            context["current"]["metrics"] = {k: evaluation["metrics"][k] for k in ("value", "rows")}
            try:
                result["reflection"] = reflect_model(context, ResearchCalls(self, f"{index:04d}-reflection"))
            except ResearchBudgetExhausted:
                result["reflection_status"] = "budget_exhausted"
            except ValueError as exc:
                result.update(reflection_status="invalid_response", reflection_error=type(exc).__name__)
        _freeze(path, result)
        return result

    def record(self, index, value):
        _freeze(self.root / "rounds" / f"{index:04d}" / "record.json", value)
        if index + 1 >= self.payload["budget"]["rounds"] or value["status"] == "stopped" or value.get("reflection_status") in {"budget_exhausted", "invalid_response"}:
            self.finish()
        return value

    def finish(self):
        history = self.history()
        passed = [row for row in history if row["status"] == "evaluated"]
        direction = 1 if self.payload["package_template"]["objective"]["direction"] == "minimize" else -1
        best = min(passed, key=lambda row: direction * row["metrics"]["value"]) if passed else None
        value = {"status": "completed" if len(history) == self.payload["budget"]["rounds"] and all(row["status"] == "evaluated" and "reflection" in row for row in history) else "incomplete",
                 "scope": "development_model_research", "campaign_id": self.payload["campaign_id"], "rounds": history,
                 "generated_candidates": sum(row["round"] > 0 for row in passed), "holdout_evaluated": False,
                 "selected_candidate_id": best["candidate_id"] if best else None,
                 "paid_model_calls": sum(_read(p)["transport"] == "live" for p in (self.root / "calls").glob("*.json"))}
        _freeze(self.root / "outcome.json", value)
        return value
