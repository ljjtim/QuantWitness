"""由上游 LoopBase 持有阶段调度、日志与会话恢复。"""
from rdagent.utils.workflow.loop import LoopBase, LoopMeta
from rdagent.core.conf import RD_AGENT_SETTINGS
from .contracts import write_json
from .scenario import RPScenario, RPTask, RPExperiment
from .execution import RPExecutionBridge
from .coder import build_coder


class RPLoop(LoopBase, metaclass=LoopMeta):
    def __init__(self, request):
        RD_AGENT_SETTINGS.step_semaphore = 1
        RD_AGENT_SETTINGS.multi_proc_n = 1
        RD_AGENT_SETTINGS.subproc_step = False
        super().__init__()
        self.request = request
        self.scenario = RPScenario(request)
        self.bridge = RPExecutionBridge(request)
        self.coder = build_coder(self.scenario, self.bridge)

    def propose(self, previous):
        return RPExperiment([RPTask(self.request)])

    def coding(self, previous):
        return self.coder.develop(previous["propose"])

    def running(self, previous):
        return previous["coding"].sub_workspace_list[0].execute()

    def feedback(self, previous):
        return previous["running"]

    def record(self, previous):
        evidence = previous["feedback"]
        write_json(self.request.session_root / "outcome.json", evidence)
        return evidence
