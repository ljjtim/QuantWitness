"""真实CoSTEER编码循环，支持固定响应与受预算约束的代码生成。"""
from copy import deepcopy
from pathlib import Path
import json
from rdagent.components.coder.CoSTEER import CoSTEER
from rdagent.components.coder.CoSTEER.config import CoSTEERSettings
from rdagent.components.coder.CoSTEER.evaluators import CoSTEERMultiFeedback, CoSTEERSingleFeedback
from rdagent.core.evolving_agent import RAGEvaluator
from rdagent.core.evolving_framework import EvolvingStrategy
from rdagent.core.experiment import Workspace
from .feedback import acceptable, candidate_needs_repair
from .contracts import write_json
from .generation import generate_source


class RPCodeWorkspace(Workspace):
    def __init__(self, bridge, candidate_id):
        super().__init__()
        self.bridge = bridge
        self.candidate_id = candidate_id
        self._checkpoint = None
        self.feedback_ref = None

    def execute(self):
        result = self.bridge.evaluate(self.candidate_id)
        if ("code_generation" in self.bridge.request.payload
                and not acceptable(result) and not candidate_needs_repair(result)):
            result = self.bridge.evaluate(self.candidate_id)
            if not acceptable(result) and not candidate_needs_repair(result):
                self.feedback_ref = result
                raise RuntimeError("原候选执行、验证或报告尚未恢复；停止模型调用，请检查候选feedback.json")
        self.feedback_ref = result
        return result

    def copy(self):
        return deepcopy(self)

    @property
    def all_codes(self):
        path = self.bridge.request.session_root / "candidates" / self.candidate_id / "compute.py"
        return path.read_text(encoding="utf-8")

    def create_ws_ckp(self):
        self._checkpoint = (self.candidate_id, deepcopy(self.feedback_ref))

    def recover_ws_ckp(self):
        if self._checkpoint is not None:
            self.candidate_id, self.feedback_ref = deepcopy(self._checkpoint)


class RPEvolvingStrategy(EvolvingStrategy):
    def __init__(self, scen, bridge):
        super().__init__(scen)
        self.bridge = bridge

    def evolve_iter(self, evo, queried_knowledge=None, evolving_trace=None):
        root = self.bridge.request.session_root
        attempt_path = root / "coder-attempts.json"
        attempts = json.loads(attempt_path.read_text(encoding="utf-8")) if attempt_path.exists() else []
        candidate_id = None
        if attempts:
            last = attempts[-1]
            receipt = root / "candidates" / last["candidate_id"] / "feedback.json"
            if not receipt.exists():
                candidate_id = last["candidate_id"]
            else:
                previous_feedback = json.loads(receipt.read_text(encoding="utf-8"))
                if acceptable(previous_feedback) or ("code_generation" in self.bridge.request.payload
                        and not candidate_needs_repair(previous_feedback)):
                    candidate_id = last["candidate_id"]
        if candidate_id is None:
            if len(attempts) >= self.bridge.request.payload["budget"]["coder_attempts"]:
                raise RuntimeError("持久编码尝试次数已耗尽")
            record = {"attempt": len(attempts)}
            knowledge = None
            if queried_knowledge is not None:
                from .coding_knowledge import query_view
                knowledge = query_view(self.bridge.request, evo.sub_tasks[0], queried_knowledge, len(attempts))
                record["coding_knowledge_records"] = [item["record_id"] for item in knowledge]
            generation = self.bridge.request.payload.get("code_generation")
            if generation and not attempts and "initial_source" in generation:
                source = generation["initial_source"]
                record["source_kind"] = "user_baseline"
            elif generation:
                previous_source = None
                evidence = None
                if attempts:
                    previous = root / "candidates" / attempts[-1]["candidate_id"]
                    previous_source = (previous / "compute.py").read_text(encoding="utf-8")
                    evidence = json.loads((previous / "feedback.json").read_text(encoding="utf-8"))
                source = generate_source(self.bridge.request, len(attempts),
                                         previous_source=previous_source, evidence=evidence, coding_knowledge=knowledge)
                record["model_call_index"] = len(attempts)
            else:
                responses = self.bridge.request.payload["runtime_binding"]["fixed_responses"]
                index = min(len(attempts), len(responses) - 1)
                source = responses[index]
                record["response_index"] = index
            candidate_id = self.bridge.candidate(source)
            record["candidate_id"] = candidate_id
            attempts.append(record)
            write_json(attempt_path, attempts)
        workspace = RPCodeWorkspace(self.bridge, candidate_id)
        evo.sub_workspace_list = [workspace]
        evo.experiment_workspace = workspace
        yield evo


class RPEvaluator(RAGEvaluator):
    def evaluate_iter(self, queried_knowledge=None, evolving_trace=None):
        evo = yield None
        evidence = evo.sub_workspace_list[0].execute()
        feedback = CoSTEERMultiFeedback([CoSTEERSingleFeedback(
            execution=str(evidence["execution_status"]),
            return_checking=str(evidence["diagnostics"]),
            code=evidence["candidate_id"], final_decision=acceptable(evidence),
        )])
        yield feedback
        return feedback


def build_coder(scenario, bridge):
    coder = CoSTEER(settings=CoSTEERSettings(max_loop=3, enable_filelock=False),
                   eva=RPEvaluator(), es=RPEvolvingStrategy(scenario, bridge),
                   scen=scenario, with_knowledge=False, knowledge_self_gen=False, max_loop=3)

    if "coding_knowledge" in bridge.request.payload:
        from .coding_knowledge import RPCodingRAG
        coder.rag = RPCodingRAG(bridge.request, coder.settings)
        coder.with_knowledge = True
        coder.knowledge_self_gen = True
    return coder
