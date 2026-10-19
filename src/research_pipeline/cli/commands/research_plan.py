"""参数与命令错误转换；业务由应用服务执行。"""

from research_pipeline.application import research_plan as _service

admit_package = _service.admit_package
