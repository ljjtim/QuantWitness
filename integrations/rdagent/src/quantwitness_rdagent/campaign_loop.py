"""RD-Agent负责跨轮调度与快照；研究收据负责幂等恢复。"""
import asyncio
from rdagent.utils.workflow.loop import LoopBase, LoopMeta
from .session_inspection import check_frozen_request, validate_campaign_request
from .generation import _MODEL_ENV_PATH
from .research_loop import configure_serial_steps, prepare_loop, wait_for_record


class ResearchCampaignLoop(LoopBase, metaclass=LoopMeta):
    def __init__(self, campaign):
        configure_serial_steps()
        super().__init__()
        self.campaign = campaign

    async def propose(self, previous):
        index = previous[self.LOOP_IDX_KEY]
        await wait_for_record(self.campaign, index, self.LoopTerminationError)
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
    payload = validate_campaign_request(payload)
    check_frozen_request(payload)
    campaign = build_campaign(payload)
    live = payload["proposer"]["mode"] == "live"
    if live != bool(model_env_file):
        raise ValueError("live需显式.env；固定策略不接收模型凭据")
    outcome = campaign.root / "outcome.json"
    if outcome.exists():
        return json.loads(outcome.read_text(encoding="utf-8"))
    loop = prepare_loop(ResearchCampaignLoop, campaign, resume=resume, session_attribute="campaign")
    with model_environment(model_env_file):
        asyncio.run(loop.run(loop_n=payload["budget"]["rounds"]))
    if not outcome.exists():
        raise RuntimeError("研究尚未完成，请使用campaign-resume恢复")
    return json.loads(outcome.read_text(encoding="utf-8"))
