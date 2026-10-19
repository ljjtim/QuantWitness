"""上游LoopBase调度联合因子与模型的提案、正式评价与研究反思。"""
from rdagent.utils.workflow.loop import LoopBase, LoopMeta

from .joint_research import JointResearch, validate_joint_research
from .session_inspection import check_frozen_request
from .research_loop import (
    configure_serial_steps, run_research_session,
    propose_research, evaluate_research, reflect_research, record_research,
)


class JointResearchLoop(LoopBase, metaclass=LoopMeta):
    def __init__(self, session):
        configure_serial_steps()
        super().__init__()
        self.session = session

    # 显式保留原阶段名与顺序，供上游LoopMeta及历史快照恢复使用。
    propose = propose_research
    evaluate = evaluate_research
    reflect = reflect_research
    record = record_research


def run_joint_research(payload, *, resume=False, model_env_file=None, step_n=None):
    payload = validate_joint_research(payload)
    check_frozen_request(payload)
    session = JointResearch(payload, model_env_file=model_env_file)
    return run_research_session(JointResearchLoop, session, resume=resume,
                                command="joint", step_n=step_n)
