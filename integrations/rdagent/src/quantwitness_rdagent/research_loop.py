"""研究循环共同的串行调度、快照恢复和完成处理。"""
import asyncio
import json


def configure_serial_steps():
    from rdagent.core.conf import RD_AGENT_SETTINGS

    RD_AGENT_SETTINGS.step_semaphore = 1
    RD_AGENT_SETTINGS.multi_proc_n = 1
    RD_AGENT_SETTINGS.subproc_step = False


async def wait_for_record(session, index, termination_error):
    # 上游可提前调度下一轮，提案只能读取已经正式落盘的反馈。
    while True:
        if (session.root / "outcome.json").exists():
            raise termination_error("研究已达到冻结停止条件")
        if index <= len(session.history()):
            return
        await asyncio.sleep(0.01)


async def propose_research(loop, previous):
    index = previous[loop.LOOP_IDX_KEY]
    await wait_for_record(loop.session, index, loop.LoopTerminationError)
    return loop.session.propose(index)


def evaluate_research(loop, previous):
    return loop.session.evaluate(previous[loop.LOOP_IDX_KEY], previous["propose"])


def reflect_research(loop, previous):
    return loop.session.reflect(previous[loop.LOOP_IDX_KEY], previous["evaluate"])


def record_research(loop, previous):
    return loop.session.record(previous[loop.LOOP_IDX_KEY], previous["reflect"])


def prepare_loop(loop_type, session, *, resume, session_attribute="session"):
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger

    RD_AGENT_SETTINGS.workspace_path = session.root / "rd-workspace"
    RD_AGENT_SETTINGS.artifact_signing_key_path = session.root / "rd-signing.key"
    LOG_SETTINGS.trace_path = str(session.root / "rd-logs")
    rdagent_logger.set_storages_path(session.root / "rd-logs")
    snapshots = session.root / "rd-logs" / "__session__"
    if resume and snapshots.exists() and any(snapshots.glob("*/*_*")):
        loop = loop_type.load(snapshots, checkout=False)
        frozen = getattr(loop, session_attribute)
        if frozen.payload != session.payload:
            raise ValueError("研究快照与冻结请求不同；请使用原请求恢复，改变研究范围须新会话")
        if session_attribute == "campaign" and frozen.data != session.data:
            raise ValueError("RD快照与冻结研究输入不一致")
        setattr(loop, session_attribute, session)
    else:
        loop = loop_type(session)
    loop.step_n = None
    return loop


def run_research_session(loop_type, session, *, resume, command, step_n=None):
    outcome = session.root / "outcome.json"

    def completed():
        if command == "factor" and "knowledge" in session.payload:
            from .research_knowledge import export_session

            export_session(session.root, session.payload["knowledge"]["output"])
        return json.loads(outcome.read_text(encoding="utf-8"))

    if outcome.exists():
        return completed()
    loop = prepare_loop(loop_type, session, resume=resume)
    asyncio.run(loop.run(step_n=step_n, loop_n=session.payload["budget"]["rounds"]))
    if outcome.exists():
        return completed()
    return {"status": "incomplete", "completed_rounds": len(session.history()),
            "next_action": command + "-resume", "session_root": str(session.root)}
