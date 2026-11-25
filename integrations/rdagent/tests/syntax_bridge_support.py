"""CoSTEER 调度测试共用的语法检查桥。"""
from quantwitness_rdagent.contracts import write_json


class SyntaxTestBridge:
    """编译源码并提供调度反馈替身，不执行候选函数或正式研究。

    return 999 是注入可修复 TypeError 的标记；成功反馈也是固定测试数据。
    本桥不验证真实返回值、公式正确性或正式 Result。
    """
    def __init__(self, request):
        from quantwitness_rdagent.execution import RPExecutionBridge
        self.delegate = RPExecutionBridge(request)
        self.request = request

    def candidate(self, source):
        return self.delegate.candidate(source)

    def evaluate(self, candidate_id):
        root = self.request.session_root / "candidates" / candidate_id
        try:
            code = (root / "compute.py").read_text(encoding="utf-8")
            compile(code, "compute.py", "exec")
            if "return 999" in code:
                raise TypeError("compute.daily_value 返回合同不符")
        except TypeError as exc:
            evidence = {'candidate_id': candidate_id, 'command_status': 'failed', 'execution_status': 'failed', 'diagnostics': [{'stage': 'run', 'error_code': 'TypeError', 'candidate_operator': True, 'message': str(exc)}]}
        except SyntaxError as exc:
            evidence = {'candidate_id': candidate_id, 'command_status': 'failed', 'execution_status': 'not_run', 'diagnostics': [{'stage': 'code_validation', 'error_type': type(exc).__name__, 'message': str(exc)}]}
        else:
            evidence = {'candidate_id': candidate_id, 'command_status': 'succeeded', 'execution_status': 'succeeded', 'result_ref': 'formal/result', 'diagnostics': []}
        write_json(root / "feedback.json", evidence)
        return evidence
