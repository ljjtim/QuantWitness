"""CoSTEER 编码经验；按确认定义或共同计算结构复用代码与修复。"""
from copy import deepcopy
import json
from pathlib import Path

from rdagent.components.coder.CoSTEER.knowledge_management import (
    CoSTEERKnowledge, CoSTEERQueriedKnowledgeV2, CoSTEERRAGStrategyV2,
)
from rdagent.components.coder.CoSTEER.evaluators import CoSTEERSingleFeedback
from rdagent.core.evolving_framework import EvolvingKnowledgeBase
from rdagent.core.serialization import load as load_knowledge
from rdagent.core.experiment import Workspace

from .contracts import FrozenRequest, write_json
from .feedback import acceptable, candidate_needs_repair, project_formula_feedback
from .generation import _technical_feedback
from .coding_transfer import formal_refs, select_records


class CodingKnowledgeBase(EvolvingKnowledgeBase):
    def __init__(self):
        self.working_trace_knowledge = {}
        self.working_trace_error_analysis = {}
        self.success_task_to_knowledge_dict = {}


class CodeSnapshot(Workspace):
    """经验只持有代码和来源标识，无执行能力。"""
    def __init__(self, source, record_id, metadata=None):
        super().__init__()
        self.source = source
        self.record_id = record_id
        self.metadata = deepcopy(metadata or {})

    @property
    def all_codes(self):
        return self.source

    def copy(self):
        return deepcopy(self)

    def create_ws_ckp(self):
        return None

    def recover_ws_ckp(self):
        return None

    def execute(self):
        raise RuntimeError("知识代码必须由当前研究包重新执行")


def definition(request):
    return json.dumps({"confirmed_formula": request.payload["confirmed_formula"],
                       "interface_contract": "pure-formula-functions-v1"},
                      ensure_ascii=False, sort_keys=True)


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _validate_formal_success(request, candidate, evidence):
    """复验 Result、公式覆盖、源码和已准入计划之间的绑定。"""
    from research_pipeline.evidence import load_verified_result_context
    from research_pipeline.results import ResultStore
    from research_pipeline.extensions import verify_project_operator_bundle, verify_project_verifier_bundle

    execution = request._local_path(evidence["execution_ref"])
    store = ResultStore(execution / "results", create=False)
    verified = load_verified_result_context(request._local_path(evidence["verification_ref"]),
                                           result_store=execution / "results")
    bundle = verified.snapshot.bundle
    if verified.verification.status != "pass":
        raise ValueError("来源公式未通过正式独立验证")
    formula = project_formula_feedback(verified, store, request.payload.get("formula_evaluation"))
    if formula["formula_status"] != "pass":
        raise ValueError("来源 Result 缺少通过的公式覆盖")
    verifier = verify_project_verifier_bundle(request._local_path(request.payload["reference_bundle"]))
    if dict(bundle.verification.verifier_identity) != verifier.identity():
        raise ValueError("来源 Verifier 与冻结请求不一致")
    if store.result_directory(bundle).resolve() != request._local_path(evidence["result_ref"]).resolve():
        raise ValueError("来源候选 Result 引用不一致")
    operator_root = request._local_path(evidence["bundle_ref"])
    operator = verify_project_operator_bundle(operator_root)
    if (operator_root / "sources/compute.py").read_text(encoding="utf-8") != (candidate / "compute.py").read_text(encoding="utf-8"):
        raise ValueError("来源代码与已验证算子源码不一致")
    plan = _read(execution / "plan/admitted/research-plan.json")
    if (plan["package_hash"] != bundle.package_hash or plan["package_plan_hash"] != bundle.plan_hash
            or operator.bundle_hash not in plan.get("project_admission", {}).get("bundle_hashes", [])):
        raise ValueError("来源代码未绑定正式 Result 计划")


def formula_task(request):
    confirmed = request.payload["confirmed_formula"]
    return {"kind": "formula", "interface": confirmed["interface"], "formula": confirmed["formula"]}


def _source_records(request, root):
    source = FrozenRequest.load(root / "request.json")
    scope = request.payload["coding_knowledge"].get("retrieval_scope", "exact_formula")
    if scope not in {"exact_formula", "technical_transfer"}:
        raise ValueError("编码知识检索范围无效")
    confirmed = source.payload.get("confirmed_formula")
    if (not confirmed or (scope == "exact_formula" and definition(source) != definition(request))
            or confirmed["interface"] != request.payload["confirmed_formula"]["interface"]):
        raise ValueError("来源知识的已确认公式或接口不一致")
    if _read(root / "frozen-inputs.json") != source._materials():
        raise ValueError("来源已冻结材料改变")
    if not acceptable(_read(root / "outcome.json")):
        raise ValueError("来源会话必须完成公式独立验证和报告")
    records = []
    for attempt in _read(root / "coder-attempts.json"):
        candidate_id = attempt["candidate_id"]
        candidate = root / "candidates" / candidate_id
        evidence = _read(candidate / "feedback.json")
        if acceptable(evidence):
            _validate_formal_success(source, candidate, evidence)
        elif not candidate_needs_repair(evidence):
            continue
        records.append({"record_id": source.payload["request_id"] + "/" + candidate_id,
                        "source": (candidate / "compute.py").read_text(encoding="utf-8"),
                        "technical_feedback": _technical_feedback(evidence), "success": acceptable(evidence),
                        "task": formula_task(source), "formal_refs": formal_refs(evidence)})
    if not any(record["success"] for record in records):
        raise ValueError("来源知识缺少已验证的修复实现")
    return records


def _knowledge(task, record):
    feedback = CoSTEERSingleFeedback(execution=record["technical_feedback"].get("execution_status", "not_run"),
        return_checking=json.dumps(record["technical_feedback"], ensure_ascii=False, sort_keys=True),
        code=record["record_id"], final_decision=record["success"])
    metadata = {key: deepcopy(record[key]) for key in ("task", "formal_refs") if key in record}
    return CoSTEERKnowledge(task, CodeSnapshot(record["source"], record["record_id"], metadata), feedback)


def _record(knowledge):
    return {"record_id": knowledge.implementation.record_id, "source": knowledge.implementation.all_codes,
            "technical_feedback": json.loads(knowledge.feedback.return_checking),
            "success": bool(knowledge.feedback.final_decision),
            **getattr(knowledge.implementation, "metadata", {})}


class RPCodingRAG(CoSTEERRAGStrategyV2):
    """复用上游 RAG 调度、历史查询和签名持久化，保留来源任务定义。"""
    def __init__(self, request, settings):
        self.request = request
        self.root = request.session_root / "coding-knowledge"
        self.root.mkdir(parents=True, exist_ok=True)
        super().__init__(settings=settings, dump_knowledge_base_path=self.root / "knowledge.pkl")

    def load_or_init_knowledge_base(self, **kwargs):
        path = self.root / "knowledge.pkl"
        if path.exists():
            with path.open("rb") as stream:
                result = load_knowledge(stream)
            if not isinstance(result, CodingKnowledgeBase):
                raise ValueError("编码知识版本不一致")
            return result
        database = CodingKnowledgeBase()
        input_path = self.root / "input.json"
        if input_path.exists():
            frozen = _read(input_path)
            if frozen["definition"] != definition(self.request):
                raise ValueError("冻结编码知识与当前定义不一致")
            records = frozen["records"]
        else:
            source = self.request.payload["coding_knowledge"].get("source_session")
            records = _source_records(self.request, self.request._local_path(source)) if source else []
            write_json(input_path, {"definition": definition(self.request), "records": records})
        from .scenario import RPTask
        task = RPTask(self.request)
        key = task.get_task_information()
        database.working_trace_knowledge[key] = [_knowledge(task, record) for record in records]
        successes = [item for item in database.working_trace_knowledge[key] if item.feedback.final_decision]
        if successes:
            database.success_task_to_knowledge_dict[key] = successes[-1]
        return database

    def query(self, evo, evolving_trace):
        queried = CoSTEERQueriedKnowledgeV2(success_task_to_knowledge_dict={}, failed_task_info_set=set(),
            task_to_former_failed_traces={}, task_to_similar_task_successful_knowledge={},
            task_to_similar_error_successful_knowledge={})
        queried = self.former_trace_query(evo, queried, self.settings.v2_query_former_trace_limit, False)
        for task in evo.sub_tasks:
            key = task.get_task_information()
            history = self.knowledgebase.working_trace_knowledge.get(key, [])
            success = self.knowledgebase.success_task_to_knowledge_dict.get(key)
            if success is not None:
                queried.task_to_former_failed_traces[key] = ([item for item in history if not item.feedback.final_decision], None)
                queried.success_task_to_knowledge_dict[key] = success
        return queried

    def generate_knowledge(self, evolving_trace, *, return_knowledge=False):
        for step in evolving_trace:
            for task, workspace in zip(step.evolvable_subjects.sub_tasks, step.evolvable_subjects.sub_workspace_list):
                evidence = workspace.feedback_ref
                if not evidence or not (acceptable(evidence) or candidate_needs_repair(evidence)):
                    continue
                key = task.get_task_information()
                records = self.knowledgebase.working_trace_knowledge.setdefault(key, [])
                record_id = self.request.payload["request_id"] + "/" + workspace.candidate_id
                if any(item.implementation.record_id == record_id for item in records):
                    continue
                record = {"record_id": record_id, "source": workspace.all_codes,
                          "technical_feedback": _technical_feedback(evidence), "success": acceptable(evidence),
                          "task": formula_task(self.request), "formal_refs": formal_refs(evidence)}
                knowledge = _knowledge(task, record)
                records.append(knowledge)
                if record["success"]:
                    self.knowledgebase.success_task_to_knowledge_dict[key] = knowledge


def query_view(request, task, queried, attempt):
    path = request.session_root / "coding-knowledge" / ("query-%04d.json" % attempt)
    if path.exists():
        return _read(path)
    key = task.get_task_information()
    failed, _ = queried.task_to_former_failed_traces.get(key, ([], None))
    success = queried.success_task_to_knowledge_dict.get(key)
    items = ([success] if success is not None else []) + list(reversed(failed))
    settings = request.payload["coding_knowledge"]
    scope = settings.get("retrieval_scope", "exact_formula")
    if scope == "technical_transfer":
        records = [_record(item) for item in reversed(items)]
        current_task = formula_task(request)
        for record in records:
            record.setdefault("task", current_task)
        result = select_records(records, current_task,
            request.payload.get("code_generation", {}).get("initial_stub", ""),
            max_records=settings["max_records"], max_source_chars=settings["max_source_chars"])
        if not success:
            result, used = [], 0
            for record in reversed(records):
                if record["task"] != current_task or len(result) >= settings["max_records"]:
                    continue
                if used + len(record["source"]) > settings["max_source_chars"]:
                    continue
                used += len(record["source"])
                result.append({key: record[key] for key in ("record_id", "source", "technical_feedback", "success")})
    else:
        result, used = [], 0
        for item in items:
            record = _record(item)
            if len(result) >= settings["max_records"]:
                break
            if used + len(record["source"]) > settings["max_source_chars"]:
                continue
            used += len(record["source"])
            result.append({key: record[key] for key in ("record_id", "source", "technical_feedback", "success")})
    write_json(path, result)
    return result


