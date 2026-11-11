"""仅显式无隐式状态因果算子可使用的节点内Worker会话。"""
from __future__ import annotations

import json
from pathlib import Path

from .errors import RuntimeIntegrityError
from .worker_process_session import WorkerProcessSession
from research_pipeline.extensions.project_bundle import _freeze_payload, _thaw_payload
from research_pipeline.platform.operator_contracts import validate_parameters


SESSION_BATCH_VERSION = "project-causal-session-batch-v1"


def session_binding(task):
    """批次只允许改变窗口、键、显式状态和剩余资源预算。"""
    context = task["context"]
    parameters = context["parameters"]
    plan = parameters.get("causal_plan", {})
    causal = task.get("causal_context", {})
    if (parameters.get("worker_lifecycle") != "stateless_session"
            or _freeze_payload(causal.get("plan")) != _freeze_payload(plan)):
        raise RuntimeIntegrityError("Worker session要求显式stateless因果声明")
    return {
        "identity": task["runtime_identity"],
        "context": {key: context[key] for key in
                    ("project_id", "run_id", "node_id", "attempt_id", "fixed_clock", "root_seed")},
        "parameters": {key: value for key, value in parameters.items() if key != "causal_plan"},
        "plan": {key: value for key, value in plan.items() if key != "work_items"},
        "inputs": task["inputs"],
        "outputs": task["outputs"],
        "source_timezones": causal["source_timezones"],
        "partition_identities": causal["partition_identities"],
        "direct_sources": causal.get("direct_sources", {}),
    }


class CausalWorkerSession(WorkerProcessSession):
    """仅显式无隐式状态因果算子使用的冻结会话。"""

    def normalize_parameters(self, parameters, specs):
        """静态声明只验证一次；可变调用仍逐项冻结后比较，不能按id猜测未变化。"""
        plan = parameters.get("causal_plan", {})
        static = _freeze_payload({
            "parameters": {key: value for key, value in parameters.items() if key != "causal_plan"},
            "plan": {key: value for key, value in plan.items() if key != "work_items"},
        })
        previous = getattr(self, "parameter_binding", None)
        if previous is None:
            normalized = _freeze_payload(validate_parameters(parameters, specs, "project.parameters"))
            self.parameter_binding = static
            self.parameter_specs = specs
            self.normalized_parameters = normalized
        elif static != previous or specs != self.parameter_specs:
            raise RuntimeIntegrityError("Worker session节点、环境或冻结输入发生变化")
        dynamic = _freeze_payload({"work_items": plan.get("work_items")})
        return _freeze_payload({**self.normalized_parameters, "causal_plan": _freeze_payload({
            **self.normalized_parameters["causal_plan"], **dynamic,
        })})

    def bind(self, task):
        if self.closed:
            raise RuntimeIntegrityError("Worker session已经关闭")
        binding = _freeze_payload(session_binding(task))
        if self.binding is None:
            self.binding = binding
        elif binding != self.binding:
            raise RuntimeIntegrityError("Worker session节点、环境或冻结输入发生变化")

    def wire_task(self, task):
        """首批完整握手，后续消息只包含本批变化的工作项、预算和显式状态。"""
        self.bind(task)
        if not getattr(self, "handshake_sent", False):
            self.handshake_sent = True
            return _thaw_payload(task)
        causal = task["causal_context"]
        return _thaw_payload({
            "contract_version": SESSION_BATCH_VERSION,
            "work_items": causal["plan"]["work_items"],
            "effective_resource_budget": task["context"]["effective_resource_budget"],
            "state_input": task["state_input"],
            "inherited_lineage": causal["inherited_lineage"],
        })



def run_session(root, execute_task):
    """每条命令完成后发布结束标记，失败不再接受下一批。"""
    import sys
    from types import SimpleNamespace
    root = Path(root).resolve(strict=True)
    cache = {"root": root}
    try:
        for line in sys.stdin.buffer:
            task_path = Path(json.loads(line)).resolve(strict=True)
            attempt_root = task_path.parent
            if not attempt_root.is_relative_to(root.parent) or attempt_root == root:
                raise ValueError("project_worker_session_path_invalid")
            args = SimpleNamespace(attempt_root=str(attempt_root), task=str(task_path),
                                   result=str(attempt_root / "result.json"))
            code = execute_task(args, session=cache)
            marker = attempt_root / "session-done.tmp"
            marker.write_text(json.dumps({"returncode": code}), encoding="utf-8")
            marker.replace(attempt_root / "session-done.json")
            if code:
                return code
    finally:
        if "archive_readers" in cache:
            cache["archive_readers"].close()
    return 0
