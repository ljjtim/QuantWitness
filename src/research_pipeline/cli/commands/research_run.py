"""参数与命令错误转换；业务由应用服务执行。"""

from research_pipeline.application import research_run as _service
from ..result import execute_guarded

def _execute(args):
    return _service._execute(args)

def execute(args):
    return execute_guarded(args, _execute)

def resume_operator_graph(*, run_root, retry_node_id=None):
    return _service.resume_operator_graph(run_root=run_root, retry_node_id=retry_node_id)
