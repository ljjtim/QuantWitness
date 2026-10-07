"""从确认基线到新表达式的研究会话，逐轮委托正式研究包。"""
from copy import deepcopy
import json
from pathlib import Path
import re

from .contracts import write_json
from .package_campaign import PackageCampaign, validate_package_campaign


class ResearchBudgetExhausted(RuntimeError):
    """研究调用预约预算已耗尽。"""


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


EXPRESSION_CONTRACT = (
    "$close / Ref($close, N) - 1 或 $close / Mean($close, N) - 1，N为1至5；"
    "Std($close, N) / Mean($close, N) 或 "
    "($close - Min($close, N)) / (Max($close, N) - Min($close, N))，N为2至5且各处窗口相同；"
    "Std为样本标准差，区间最高最低相等时缺失；只用决策前一已完成会话及更早收盘价"
)


CLOSE_FACTOR_CONTRACT = "close-price-v1"
DAI_FACTOR_CONTRACT = "dai-daily-v1"
DAI_EXPRESSION_CONTRACT = (
    "$dai为已确认分钟事件规则及20交易会话聚合形成的日级值；"
    "仅允许基线$dai、Ref($dai, N)，N为1至5，或Mean($dai, N)，N为2至5。"
    "D日09:30只使用前一交易会话及更早完整输入；零值合法，缺失不前填、不压缩日历。"
    "全部候选共用最多25交易会话完整历史的合格掩码，资格在Fillna之前判断。"
)


def factor_contract(value):
    """已确认日级合同显式声明，既有价格合同保持默认。"""
    contract = value.get("factor_contract", CLOSE_FACTOR_CONTRACT)
    if contract not in {CLOSE_FACTOR_CONTRACT, DAI_FACTOR_CONTRACT}:
        raise ValueError("不支持的因子合同")
    return contract


def feature_expression_slot(design):
    return "dai_following" if factor_contract(design) == DAI_FACTOR_CONTRACT else "historical_return"


def factor_directions(contract):
    return ("lag", "mean") if contract == DAI_FACTOR_CONTRACT else ("momentum", "mean_deviation", "relative_volatility", "range_position")


def canonical_expression(expression, *, contract=CLOSE_FACTOR_CONTRACT):
    """表达式仅在本次明确声明的因子合同中解释。"""
    if not isinstance(expression, str):
        raise ValueError("研究表达式必须为文本")
    compact = "".join(expression.split())
    if contract == DAI_FACTOR_CONTRACT:
        if compact == "$dai":
            return "$dai"
        match = re.fullmatch(r"Ref\(\$dai,([1-5])\)", compact)
        if match:
            return f"Ref($dai, {match[1]})"
        match = re.fullmatch(r"Mean\(\$dai,([2-5])\)", compact)
        if match:
            return f"Mean($dai, {match[1]})"
        raise ValueError("日级因子仅允许$dai、Ref($dai, 1..5)或Mean($dai, 2..5)")
    if contract != CLOSE_FACTOR_CONTRACT:
        raise ValueError("不支持的因子合同")
    match = re.fullmatch(r"\$close/(Ref|Mean)\(\$close,([1-5])\)-1(?:\.0)?", compact)
    if match:
        return f"$close / {match[1]}($close, {match[2]}) - 1"
    match = re.fullmatch(r"Std\(\$close,([2-5])\)/Mean\(\$close,\1\)", compact)
    if match:
        return f"Std($close, {match[1]}) / Mean($close, {match[1]})"
    match = re.fullmatch(r"\(\$close-Min\(\$close,([2-5])\)\)/\(Max\(\$close,\1\)-Min\(\$close,\1\)\)", compact)
    if match:
        return f"($close - Min($close, {match[1]})) / (Max($close, {match[1]}) - Min($close, {match[1]}))"
    raise ValueError("表达式必须符合已复核的收盘动量、均价偏离、相对波动或区间位置定义")


def expression_direction(expression, *, contract=CLOSE_FACTOR_CONTRACT):
    """方向由规范公式决定，不以模型给出的名称替代。"""
    expression = canonical_expression(expression, contract=contract)
    if contract == DAI_FACTOR_CONTRACT:
        return "baseline" if expression == "$dai" else "lag" if expression.startswith("Ref(") else "mean"
    if expression.startswith("$close / Ref("):
        return "momentum"
    if expression.startswith("$close / Mean("):
        return "mean_deviation"
    if expression.startswith("Std("):
        return "relative_volatility"
    return "range_position"


def validate_factor_research(payload):
    fields = {"contract_version", "research_kind", "campaign_id", "session_root", "package_template",
              "confirmed_spec", "baseline", "budget", "proposer"}
    if not isinstance(payload, dict) or not fields <= set(payload) or set(payload) - fields - {"knowledge", "factor_contract"}:
        raise ValueError("因子研究请求字段不完整")
    value = json.loads(json.dumps(payload, allow_nan=False))
    contract = factor_contract(value)
    if "knowledge" in value:
        knowledge = value["knowledge"]
        if (not isinstance(knowledge, dict) or set(knowledge) != {"index", "output", "max_records"}
                or not isinstance(knowledge["index"], str) or not knowledge["index"]
                or not isinstance(knowledge["output"], str) or not knowledge["output"]
                or type(knowledge["max_records"]) is not int or not 1 <= knowledge["max_records"] <= 32):
            raise ValueError("knowledge必须声明输入、输出索引路径及1至32条检索上限")
    if value["contract_version"] != "rd-factor-research-v1" or value["research_kind"] != "factor_research":
        raise ValueError("不支持的因子研究合同")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value["campaign_id"]):
        raise ValueError("campaign_id必须是简单标识")
    if not isinstance(value["session_root"], str) or not value["session_root"]:
        raise ValueError("必须提供会话目录")
    template = validate_package_campaign(value["package_template"])
    if template["proposer"] != {"mode": "fixed_policy"} or len(template["candidates"]) != 1:
        raise ValueError("执行模板只能含一个基准；新候选由研究提案产生")
    value["package_template"] = template
    baseline = value["baseline"]
    if not isinstance(baseline, dict) or set(baseline) != {"expression", "hypothesis", "reason"}:
        raise ValueError("基准必须声明公式、假设与理由")
    baseline["expression"] = canonical_expression(baseline["expression"], contract=contract)
    if contract == DAI_FACTOR_CONTRACT and baseline["expression"] != "$dai":
        raise ValueError("日级因子研究基线必须为已确认的$dai")
    if any(not isinstance(baseline[k], str) or not baseline[k].strip() for k in ("hypothesis", "reason")):
        raise ValueError("基准假设与理由不能为空")
    confirmation = value["confirmed_spec"]
    if not isinstance(confirmation, dict) or confirmation.get("kind") not in {"synthetic", "confirmed_formula"}:
        raise ValueError("需提供确认过的文档定义或明确的合成教学定义")
    required = {"kind", "path"} if confirmation["kind"] == "synthetic" else {"kind", "path", "draft", "materials", "decisions"}
    if set(confirmation) != required or any(not isinstance(confirmation[k], str) or not confirmation[k] for k in required):
        raise ValueError("确认材料路径不完整")
    budget = value["budget"]
    if not isinstance(budget, dict) or set(budget) != {"rounds", "model_calls", "output_tokens", "max_output_tokens_per_call"}:
        raise ValueError("研究预算必须完整")
    if any(type(v) is not int or v < 1 for v in budget.values()) or not 1 <= budget["max_output_tokens_per_call"] <= 8192:
        raise ValueError("研究预算必须为正整数且单次输出不超过8192")
    proposer = value["proposer"]
    if proposer.get("mode") == "fixed_responses":
        if set(proposer) != {"mode", "responses"} or not isinstance(proposer["responses"], list) or not proposer["responses"]:
            raise ValueError("固定响应需要按调用顺序提供文本文件")
        if any(not isinstance(x, str) or not x for x in proposer["responses"]):
            raise ValueError("固定响应路径无效")
    elif proposer.get("mode") == "live":
        from urllib.parse import urlsplit
        if set(proposer) != {"mode", "model", "base_url"} or not isinstance(proposer["model"], str) or not proposer["model"]:
            raise ValueError("live模型身份不完整")
        endpoint = urlsplit(proposer["base_url"])
        if endpoint.scheme != "https" or not endpoint.netloc or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            raise ValueError("live接口地址无效")
    else:
        raise ValueError("只支持live或固定文本响应")
    return value


def _freeze(path, value):
    path = Path(path)
    if path.exists():
        if _read(path) != value:
            raise ValueError("冻结材料已改变: " + path.name)
    else:
        write_json(path, value)


class ResearchCalls:
    """每个阶段按调用序号封存请求和响应；未知结果不自动再付费。"""
    def __init__(self, session, phase):
        self.session = getattr(session, "call_owner", session)
        self.phase = getattr(session, "call_prefix", "") + phase
        self.index = 0

    def __call__(self, prompt, *, instructions, max_output_tokens=None):
        session = self.session
        budget = session.payload["budget"]
        limit = min(max_output_tokens or budget["max_output_tokens_per_call"], budget["max_output_tokens_per_call"])
        key = self.phase + "-" + str(self.index).zfill(2)
        self.index += 1
        path = session.root / "calls" / (key + ".json")
        request = {"prompt": prompt, "instructions": instructions, "max_output_tokens": limit}
        if path.exists():
            saved = _read(path)
            if saved["request"] != request:
                raise ValueError("恢复模型请求与已预约内容不同")
            if saved["status"] == "completed":
                return saved["response"]
            raise RuntimeError("model_call_outcome_unknown: 请核对已预约调用，当前会话不自动重复请求")
        if session.payload["proposer"]["mode"] == "live":
            from .model_client import public_config
            if public_config(session.model_env_file) != {k: session.payload["proposer"][k] for k in ("model", "base_url")}:
                raise ValueError(".env模型身份在会话内改变")
        calls = [_read(p) for p in (session.root / "calls").glob("*.json")]
        if len(calls) >= budget["model_calls"] or sum(c["request"]["max_output_tokens"] for c in calls) + limit > budget["output_tokens"]:
            raise ResearchBudgetExhausted("model_budget")
        receipt = {"status": "reserved", "request": request, "call_index": len(calls),
                   "transport": session.payload["proposer"]["mode"]}
        write_json(path, receipt)
        if receipt["transport"] == "fixed_responses":
            responses = session.materials["responses"]
            if len(calls) >= len(responses):
                raise RuntimeError("固定响应数量不足")
            response = responses[len(calls)]
        else:
            from .model_client import request_text
            try:
                result = request_text(session.model_env_file, prompt, limit, instructions=instructions)
                if result.get("model") != session.payload["proposer"]["model"]:
                    raise ValueError("模型响应身份与冻结请求不同")
                response = result.get("text")
                if not isinstance(response, str) or not response.strip():
                    raise ValueError("模型未返回有效文本")
                usage = result.get("usage") or {}
                if type(usage.get("output_tokens")) is int and usage["output_tokens"] > limit:
                    raise ValueError("模型输出超过预约预算")
                receipt["usage"] = {key: value for key, value in usage.items()
                    if key in ("input_tokens", "output_tokens", "total_tokens") and type(value) is int and value >= 0}
            except Exception:
                receipt.update(status="failed", error_code="model_call_failed")
                write_json(path, receipt)
                raise RuntimeError("模型调用失败，预算已占用；核对收据后处理，不自动重复付费") from None
        if not isinstance(response, str) or not response.strip():
            raise ValueError("模型未返回有效文本")
        receipt.update(status="completed", response=response)
        write_json(path, receipt)
        return response


class FactorResearch:
    def __init__(self, payload, *, model_env_file=None):
        self.payload = validate_factor_research(payload)
        self.factor_contract = factor_contract(self.payload)
        self.root = Path(self.payload["session_root"]).resolve()
        self.model_env_file = model_env_file
        live = self.payload["proposer"]["mode"] == "live"
        if live != bool(model_env_file):
            raise ValueError("live必须显式提供.env，固定响应不接收凭据")
        if live:
            from .model_client import public_config
            identity = public_config(model_env_file)
            if identity != {k: self.payload["proposer"][k] for k in ("model", "base_url")}:
                raise ValueError(".env模型身份与冻结请求不符")
        from research_pipeline.packages import load_research_package
        from research_pipeline.data_plane import PathRolePolicy
        template = self.payload["package_template"]
        source = template["source"]
        paths = {"output": str(self.root), **{k: source[k] for k in ("path", "catalog_lock", "verifier_bundle", "input_snapshot_manifest", "source_archive_root")}}
        paths.update({"extension_"+str(i): x for i,x in enumerate(source["extension_bundles"])})
        paths.update({"confirmation_"+k: v for k,v in self.payload["confirmed_spec"].items() if k != "kind"})
        paths.update({"response_"+str(i): x for i,x in enumerate(self.payload["proposer"].get("responses", []))})
        if "knowledge" in self.payload:
            paths["knowledge_index"] = self.payload["knowledge"]["index"]
            paths["knowledge_output"] = self.payload["knowledge"]["output"]
            if Path(paths["knowledge_output"]).resolve().is_relative_to(self.root):
                raise ValueError("知识输出索引必须在本会话目录之外")
        PathRolePolicy().validate(paths, read_only_roles=tuple(k for k in paths if k not in {"output", "knowledge_output"}))
        package = load_research_package(source["path"])
        nodes = {n["node_id"]: n for n in package.spec_payload["graph"]["nodes"]}
        if not {"feature", "label", "summary", "model_fit"} <= set(nodes):
            raise ValueError("首例要求日频Qlib研究包的feature/label/summary/model_fit节点")
        self.design = json.loads(json.dumps(nodes["feature"]["parameters"]["design"], default=dict))
        if factor_contract(self.design) != self.factor_contract:
            raise ValueError("因子请求与正式设计的合同不同")
        if self.factor_contract == DAI_FACTOR_CONTRACT and self.design.get("feature_expressions") != {"dai_following": "$dai"}:
            raise ValueError("日级模板必须声明唯一dai_following槽及$dai基线")
        from research_pipeline.extensions.verifier_bundle import verify_project_verifier_bundle
        verifier = verify_project_verifier_bundle(source["verifier_bundle"])
        definitions = [definition for definition in verifier.metric_definitions
                       if definition.metric_ref == self.design["metric_ref"] and
                       definition.result_schema_id == template["objective"]["schema_id"]]
        if len(definitions) != 1:
            raise ValueError("开发目标缺少唯一正式指标定义")
        definition = definitions[0].payload()
        self.metric_description = {key: definition[key] for key in
            ("metric_id", "unit", "frequency", "direction", "annualization_policy", "measurement_semantics")}
        if ((template["objective"]["direction"] == "minimize") != (definition["direction"] == "lower_is_better")):
            raise ValueError("研究目标方向与正式指标定义不同")
        if self.design.get("mode") != "development":
            raise ValueError("研究只接受development包")
        confirmation = self.payload["confirmed_spec"]
        if confirmation["kind"] == "synthetic":
            document = _read(confirmation["path"])
            if (document.get("status") != "confirmed" or document.get("kind") != "synthetic" or
                    document.get("formula") != self.payload["baseline"]["expression"] or
                    self.design.get("snapshot_scope") != "public_synthetic_no_market_claim" or
                    not isinstance(document.get("text"), str) or not document["text"].strip()):
                raise ValueError("教学文档、基准表达式及合成数据范围不匹配")
            text = document["text"]
        else:
            from .formula_spec import render_spec
            text = render_spec(confirmation["draft"], confirmation["materials"], confirmation["decisions"], confirmation["path"])
            document = {k: _read(v) for k,v in confirmation.items() if k != "kind"}
            interface = document["decisions"]["interface"]
            if self.payload["baseline"]["expression"] not in interface:
                raise ValueError("确认过的interface必须明确包含本次规范基线表达式")
        self.materials = {"document": document, "text": text,
            "responses": [Path(x).read_text(encoding="utf-8") for x in self.payload["proposer"].get("responses", [])]}
        _freeze(self.root / "request.json", self.payload)
        _freeze(self.root / "materials.json", self.materials)
        # 模板走现有包冻结逻辑，包括Catalog、bundle和归档声明。
        frozen = deepcopy(template)
        frozen["session_root"] = str(self.root / "frozen-template")
        self.template = PackageCampaign(frozen)
        self.knowledge_records = []
        if "knowledge" in self.payload:
            from .research_knowledge import query_index
            knowledge = self.payload["knowledge"]
            index = _read(knowledge["index"])
            receipt = self.root / "knowledge-input.json"
            if receipt.exists():
                saved = _read(receipt)
                if saved["index"] != index:
                    raise ValueError("冻结知识索引已改变")
                self.knowledge_records = saved["records"]
            else:
                self.knowledge_records = query_index(knowledge["index"],
                    development=template["development"], objective=template["objective"],
                    metric_description=self.metric_description, design=self.design,
                    max_records=knowledge["max_records"])
                _freeze(receipt, {"index": index, "records": self.knowledge_records})

    def history(self):
        return [_read(p) for p in sorted((self.root / "rounds").glob("*/record.json"))]

    def context(self):
        template = self.payload["package_template"]
        history = []
        for row in self.history():
            item = {key: row[key] for key in ("candidate_id", "expression", "hypothesis", "reason", "status") if key in row}
            if row["status"] == "evaluated":
                item["metrics"] = {k: row["metrics"][k] for k in ("value", "rows")}
            if "reflection" in row:
                item["reflection"] = row["reflection"]
            history.append(item)
        context = {"confirmed_spec": self.materials["text"], "fields": ["close"], "max_window": 5,
                "expression_contract": EXPRESSION_CONTRACT,
                "development": template["development"], "objective": {**template["objective"], "metric_definition": self.metric_description}, "history": history}
        if factor_contract(self.payload) == DAI_FACTOR_CONTRACT:
            context.update(factor_contract=DAI_FACTOR_CONTRACT, fields=["dai"], expression_contract=DAI_EXPRESSION_CONTRACT)
        if "knowledge" in self.payload:
            from .direction_selection import knowledge_view
            context["knowledge"] = knowledge_view(self.knowledge_records)
        return context

    def propose(self, index):
        path = self.root / "rounds" / f"{index:04d}" / "proposal.json"
        if path.exists():
            return _read(path)
        if index == 0:
            proposal = deepcopy(self.payload["baseline"])
        else:
            from .native_research import propose_factor
            try:
                context = self.context()
                if "knowledge" in self.payload:
                    from .direction_selection import select_direction
                    direction_path = self.root / "rounds" / f"{index:04d}" / "direction.json"
                    if direction_path.exists():
                        direction = _read(direction_path)
                    else:
                        direction = select_direction(context, ResearchCalls(self, f"{index:04d}-direction"))
                        _freeze(direction_path, direction)
                    if direction["action"] == "stop":
                        proposal = {"action": "stop", "reason": direction["reason"]}
                    else:
                        context["direction"] = direction
                        proposal = propose_factor(context, ResearchCalls(self, f"{index:04d}-proposal"))
                        proposal["direction"] = direction
                else:
                    proposal = propose_factor(context, ResearchCalls(self, f"{index:04d}-proposal"))
            except ResearchBudgetExhausted:
                proposal = {"action": "stop", "reason": "model_budget"}
            except (ValueError, json.JSONDecodeError) as exc:
                proposal = {"action": "reject", "reason": "proposal_invalid", "error_type": type(exc).__name__}
        if proposal.get("action") not in {"stop", "reject"}:
            try:
                fields = {"expression", "hypothesis", "reason"}
                if index > 0 and "knowledge" in self.payload:
                    fields.update({"direction", "knowledge_refs"})
                if set(proposal) != fields:
                    raise ValueError("提案字段无效")
                proposal["expression"] = canonical_expression(proposal["expression"], contract=factor_contract(self.payload))
                if any(not isinstance(proposal[k], str) or not proposal[k].strip() for k in ("hypothesis", "reason")):
                    raise ValueError("提案说明无效")
                prior = self.history()
                if index > 0 and "knowledge" in self.payload:
                    prior += self.knowledge_records
                if proposal["expression"] in {canonical_expression(row["expression"], contract=factor_contract(self.payload)) for row in prior if row.get("expression")}:
                    raise ValueError("提案表达式已评价")
            except ValueError:
                proposal = {"action": "reject", "reason": "unsupported_or_repeated_expression"}
        proposal.update(candidate_id=f"factor_{index:04d}", round=index)
        write_json(path, proposal)
        return proposal

    def _candidate_payload(self, proposal):
        from research_pipeline.platform import typed_canonical_hash
        payload = deepcopy(self.payload["package_template"])
        design = deepcopy(self.design)
        design["feature_expressions"] = {feature_expression_slot(design): canonical_expression(proposal["expression"], contract=factor_contract(self.payload))}
        identity = typed_canonical_hash(design)
        overrides = [{"node_id": "model_fit", "parameter_name": "research_identity_hash", "value": identity}]
        for name in ("feature", "label", "summary"):
            overrides.append({"node_id": name, "parameter_name": "design", "value": design})
            if name != "summary":
                overrides.append({"node_id": name, "parameter_name": "lineage_ref", "value": identity})
        payload.update(campaign_id=self.payload["campaign_id"]+"-"+proposal["candidate_id"],
            session_root=str(self.root / "experiments" / proposal["candidate_id"]),
            candidates=[{"id": proposal["candidate_id"], "parameter_overrides": overrides}], baseline_id=proposal["candidate_id"])
        return payload

    def evaluate(self, index, proposal):
        path = self.root / "rounds" / f"{index:04d}" / "evaluation.json"
        if path.exists():
            return _read(path)
        if self.template.prepare_data() != self.template.data:
            raise ValueError("基准包或输入已改变")
        result = dict(proposal)
        if proposal.get("action") in {"stop", "reject"}:
            result["status"] = "stopped" if proposal["action"] == "stop" else "rejected"
        else:
            session = PackageCampaign(self._candidate_payload(proposal))
            result["parent_id"] = self.history()[-1]["candidate_id"] if self.history() else None
            try:
                metrics = session.evaluate_metrics(session.payload["candidates"][0])
                result.update(status="evaluated", metrics=metrics)
            except ValueError as exc:
                result.update(status="failed", reason="formal_verification_or_metric_failed", error_type=type(exc).__name__)
        write_json(path, result)
        return result

    def reflect(self, index, evaluation):
        path = self.root / "rounds" / f"{index:04d}" / "reflection.json"
        if path.exists():
            return _read(path)
        result = dict(evaluation)
        if evaluation["status"] == "evaluated":
            from .native_research import reflect_factor
            context = self.context()
            context["current"] = {k: evaluation[k] for k in ("candidate_id", "expression", "hypothesis", "reason", "status")}
            context["current"]["metrics"] = {k: evaluation["metrics"][k] for k in ("value", "rows")}
            try:
                result["reflection"] = reflect_factor(context, ResearchCalls(self, f"{index:04d}-reflection"))
            except ResearchBudgetExhausted:
                result["reflection_status"] = "budget_exhausted"
            except (ValueError, json.JSONDecodeError) as exc:
                result.update(reflection_status="invalid_response", reflection_error=type(exc).__name__)
        else:
            result["reflection_status"] = "not_evaluated"
        write_json(path, result)
        return result

    def record(self, index, value):
        _freeze(self.root / "rounds" / f"{index:04d}" / "record.json", value)
        reason = None
        if index == 0 and value["status"] != "evaluated":
            reason = "baseline_failed"
        elif value.get("reflection_status") in {"budget_exhausted", "invalid_response"}:
            reason = value["reflection_status"]
        elif value["status"] == "stopped":
            reason = value["reason"]
        elif index + 1 >= self.payload["budget"]["rounds"]:
            reason = "round_budget"
        if reason:
            self.finish(reason)
        return value

    def finish(self, reason):
        output = self.root / "outcome.json"
        if output.exists():
            return _read(output)
        history = self.history()
        passed = [r for r in history if r["status"] == "evaluated"]
        direction = 1 if self.payload["package_template"]["objective"]["direction"] == "minimize" else -1
        best = min(passed, key=lambda r: direction * r["metrics"]["value"]) if passed else None
        complete = (len(history) == self.payload["budget"]["rounds"] and
                    all(r["status"] == "evaluated" and "reflection" in r for r in history))
        result = {"status": "completed" if complete else "incomplete", "stop_reason": reason,
                  "scope": "development_factor_research", "campaign_id": self.payload["campaign_id"],
                  "rounds": history, "new_formal_result": bool(passed), "holdout_evaluated": False,
                  "generated_candidates": sum(r["round"] > 0 for r in passed),
                  "selected_candidate_id": best["candidate_id"] if best else None,
                  "selected_result_ref": best["metrics"]["result_ref"] if best else None,
                  "selected_verification_ref": best["metrics"]["verification_ref"] if best else None,
                  "paid_model_calls": sum(_read(p)["transport"] == "live" for p in (self.root / "calls").glob("*.json"))}
        write_json(output, result)
        return result
