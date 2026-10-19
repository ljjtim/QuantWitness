"""仅显式无隐式状态因果算子可使用的节点内Worker会话。"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess

from .errors import RuntimeIntegrityError, RuntimeWorkerError


def session_binding(task):
    """批次只允许改变窗口、键、显式状态和剩余资源预算。"""
    context = task["context"]
    parameters = context["parameters"]
    plan = parameters.get("causal_plan", {})
    causal = task.get("causal_context", {})
    if parameters.get("worker_lifecycle") != "stateless_session" or causal.get("plan") != plan:
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
    }


class CausalWorkerSession:
    """一个节点attempt内串行执行；离开作用域时必须关闭进程树。"""

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.process = None
        self.identity = None
        self.registry = None
        self.implementation_id = None
        self.binding = None
        self.observed_descendants = {}
        self.cleanup_status = None
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def bind(self, task):
        if self.closed:
            raise RuntimeIntegrityError("Worker session已经关闭")
        binding = session_binding(task)
        if self.binding is None:
            self.binding = copy.deepcopy(binding)
        elif binding != self.binding:
            raise RuntimeIntegrityError("Worker session节点、环境或冻结输入发生变化")

    def dispatch(self, command, task_path):
        if self.process is None:
            self.process = subprocess.Popen(
                command[:3] + ["--session-root", str(self.root)],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        if self.process.poll() is not None:
            raise RuntimeWorkerError("project_worker_session_exited", error_code="project_worker_session_exited")
        try:
            self.process.stdin.write((json.dumps(str(task_path)) + "\n").encode("utf-8"))
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            self.close()
            raise RuntimeWorkerError("project_worker_session_exited", error_code="project_worker_session_exited") from exc
        return self.process

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.process is None:
            self.cleanup_status = "complete"
            return
        from research_pipeline.platform.process_resources import _terminate_process_tree
        if self.process.stdin is not None:
            self.process.stdin.close()
        self.cleanup_status = _terminate_process_tree(
            self.process, observed_descendants=self.observed_descendants,
        )
        if self.cleanup_status != "complete":
            raise RuntimeWorkerError("project_worker_cleanup_failed", error_code="project_worker_cleanup_failed")


def run_session(root, execute_task):
    """每条命令完成后发布结束标记，失败不再接受下一批。"""
    import sys
    from types import SimpleNamespace
    root = Path(root).resolve(strict=True)
    cache = {"root": root}
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
    return 0
