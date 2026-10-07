"""上游LoopBase调度生成模型的提案、正式评价与研究反思。"""
import asyncio
import json
from pathlib import Path

from rdagent.core.conf import RD_AGENT_SETTINGS
from rdagent.utils.workflow.loop import LoopBase, LoopMeta

from .model_research import ModelResearch


class ModelResearchLoop(LoopBase, metaclass=LoopMeta):
    def __init__(self, session):
        RD_AGENT_SETTINGS.step_semaphore = 1
        RD_AGENT_SETTINGS.multi_proc_n = 1
        RD_AGENT_SETTINGS.subproc_step = False
        super().__init__()
        self.session = session

    async def propose(self, previous):
        index = previous[self.LOOP_IDX_KEY]
        while index > len(self.session.history()):
            await asyncio.sleep(0.01)
        if (self.session.root / "outcome.json").exists():
            raise self.LoopTerminationError("已到研究停止条件")
        return self.session.propose(index)

    def evaluate(self, previous):
        return self.session.evaluate(previous[self.LOOP_IDX_KEY], previous["propose"])

    def reflect(self, previous):
        return self.session.reflect(previous[self.LOOP_IDX_KEY], previous["evaluate"])

    def record(self, previous):
        return self.session.record(previous[self.LOOP_IDX_KEY], previous["reflect"])


def run_model_research(payload, *, resume=False, model_env_file=None, step_n=None):
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    session = ModelResearch(payload, model_env_file=model_env_file)
    outcome = session.root / "outcome.json"
    def completed():
        return json.loads(outcome.read_text(encoding="utf-8"))
    if outcome.exists():
        return completed()
    RD_AGENT_SETTINGS.workspace_path = session.root / "rd-workspace"
    RD_AGENT_SETTINGS.artifact_signing_key_path = session.root / "rd-signing.key"
    LOG_SETTINGS.trace_path = str(session.root / "rd-logs")
    rdagent_logger.set_storages_path(session.root / "rd-logs")
    snapshots = session.root / "rd-logs" / "__session__"
    if resume and snapshots.exists() and any(snapshots.glob("*/*_*")):
        loop = ModelResearchLoop.load(snapshots, checkout=False)
        if loop.session.payload != session.payload:
            raise ValueError("研究快照与冻结请求不同")
        loop.session = session
    else:
        loop = ModelResearchLoop(session)
    loop.step_n = None
    asyncio.run(loop.run(step_n=step_n, loop_n=session.payload["budget"]["rounds"]))
    if outcome.exists():
        return completed()
    return {"status": "incomplete", "completed_rounds": len(session.history()),
            "next_action": "model-resume", "session_root": str(session.root)}
