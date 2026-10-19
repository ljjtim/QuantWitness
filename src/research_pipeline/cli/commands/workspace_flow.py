"""参数与命令错误转换；业务由应用服务执行。"""

from research_pipeline.application import workspace_flow as _service

execute_workspace = _service.execute_workspace
