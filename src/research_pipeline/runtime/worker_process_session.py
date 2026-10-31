"""节点内持续Worker的传输与进程树清理。"""
from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
import subprocess

from .errors import RuntimeWorkerError


class WorkerProcessSession:
    """复用一个子进程，调用方负责冻结任务身份和消息合同。"""

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.process = None
        self.identity = None
        self.registry = None
        self.implementation_id = None
        self.binding = None
        self.observed_descendants = {}
        self.windows_job_name = ("Local\\QuantWitness-worker-" + uuid.uuid4().hex
                                 if sys.platform == "win32" else None)
        self.cleanup_status = None
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def dispatch(self, command, task_path):
        if self.process is None:
            from research_pipeline.platform.process_resources import _WINDOWS_WORKER_JOB_ENV
            environment = os.environ.copy()
            if self.windows_job_name is not None:
                environment[_WINDOWS_WORKER_JOB_ENV] = self.windows_job_name
            self.process = subprocess.Popen(
                command[:3] + ["--session-root", str(self.root)],
                env=environment, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
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

