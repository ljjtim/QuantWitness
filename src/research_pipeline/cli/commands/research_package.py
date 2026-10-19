"""参数与命令错误转换；业务由应用服务执行。"""

from research_pipeline.application import research_package as _service
from ..result import execute_guarded

def _execute(args):
    return _service._execute(args)

def execute(args):
    return execute_guarded(args, _execute)
