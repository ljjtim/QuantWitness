"""固定公式目标的 RD 场景，不向生成器暴露收益标签。"""
from rdagent.core.scenario import Scenario
from rdagent.core.experiment import Task, Experiment


class RPScenario(Scenario):
    def __init__(self, request):
        self.request = request

    @property
    def background(self):
        return "按冻结公式、时段和缺失政策复现因子，只修改 compute.py。"

    @property
    def rich_style_description(self):
        return self.background

    def get_scenario_all_desc(self, task=None, filtered_tag=None, simple_background=None):
        return self.background

    def get_runtime_environment(self):
        mode = "受持久预算约束的模型代码生成" if "code_generation" in self.request.payload else "零实时模型调用"
        return "Linux RD-Agent；Windows RP正式执行；" + mode + "。"


class RPTask(Task):
    def __init__(self, request):
        super().__init__(name=request.payload["request_id"], description="冻结公式复现；评价只使用开发区技术诊断。")


class RPExperiment(Experiment):
    pass
