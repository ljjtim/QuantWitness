"""RD-Agent负责跨轮调度与快照；研究收据负责幂等恢复。"""
import asyncio
from rdagent.core.conf import RD_AGENT_SETTINGS
from rdagent.utils.workflow.loop import LoopBase, LoopMeta
from .generation import _MODEL_ENV_PATH


class ResearchCampaignLoop(LoopBase, metaclass=LoopMeta):
    def __init__(self, campaign):
        RD_AGENT_SETTINGS.step_semaphore = 1
        RD_AGENT_SETTINGS.multi_proc_n = 1
        RD_AGENT_SETTINGS.subproc_step = False
        super().__init__()
        self.campaign = campaign

    async def propose(self, previous):
        index = previous[self.LOOP_IDX_KEY]
        # 上游可提前调度下一轮；假设必须等待前一轮反馈正式落盘。
        while index > len(self.campaign.history()):
            await asyncio.sleep(0.01)
        if (self.campaign.root / "outcome.json").exists():
            raise self.LoopTerminationError("研究已达到冻结停止条件")
        reason = self.campaign.stop_reason()
        if reason:
            self.campaign.finish(reason)
            raise self.LoopTerminationError(reason)
        return self.campaign.propose(index, _MODEL_ENV_PATH.get())

    def evaluate(self, previous):
        return self.campaign.evaluate(previous[self.LOOP_IDX_KEY], previous["propose"])

    def feedback(self, previous):
        return previous["evaluate"]

    def record(self, previous):
        return self.campaign.record(previous[self.LOOP_IDX_KEY], previous["feedback"])


def run_campaign(payload, *, resume=False, model_env_file=None):
    import json
    from .campaign import build_campaign
    from .generation import model_environment
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    campaign = build_campaign(payload)
    live = payload["proposer"]["mode"] == "live"
    if live != bool(model_env_file):
        raise ValueError("live需显式.env；固定策略不接收模型凭据")
    outcome = campaign.root / "outcome.json"
    if outcome.exists():
        return json.loads(outcome.read_text(encoding="utf-8"))
    RD_AGENT_SETTINGS.workspace_path = campaign.root / "rd-workspace"
    RD_AGENT_SETTINGS.artifact_signing_key_path = campaign.root / "rd-signing.key"
    LOG_SETTINGS.trace_path = str(campaign.root / "rd-logs")
    rdagent_logger.set_storages_path(campaign.root / "rd-logs")
    snapshots = campaign.root / "rd-logs" / "__session__"
    if resume and snapshots.exists() and any(snapshots.glob("*/*_*")):
        loop = ResearchCampaignLoop.load(snapshots, checkout=False)
        if loop.campaign.payload != campaign.payload or loop.campaign.data != campaign.data:
            raise ValueError("RD快照与冻结研究输入不一致")
        loop.campaign = campaign
    else:
        loop = ResearchCampaignLoop(campaign)
    loop.step_n = None
    with model_environment(model_env_file):
        asyncio.run(loop.run(loop_n=payload["budget"]["rounds"]))
    if not outcome.exists():
        raise RuntimeError("研究尚未完成，请使用campaign-resume恢复")
    return json.loads(outcome.read_text(encoding="utf-8"))
