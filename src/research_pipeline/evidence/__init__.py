"""正式结果的时间序列分析接口。"""
from importlib import import_module

__all__ = ['EvidenceContractError', 'ANALYSIS_REQUEST_VERSION', 'ANALYSIS_RESULT_VERSION', 'ANALYSIS_COMPARISON_VERSION', 'AnalysisComparison', 'AnalysisRequest', 'AnalysisResult', 'analyze_result', 'compare_analysis_results', 'load_analysis_comparison', 'load_analysis_request', 'load_analysis_result', 'write_analysis_comparison', 'write_analysis_result']

_LAZY_EXPORTS = {'EvidenceContractError': '.errors', 'ANALYSIS_REQUEST_VERSION': '.analysis_contracts', 'ANALYSIS_RESULT_VERSION': '.analysis_contracts', 'ANALYSIS_COMPARISON_VERSION': '.analysis_contracts', 'AnalysisComparison': '.analysis_contracts', 'AnalysisRequest': '.analysis_contracts', 'AnalysisResult': '.analysis_contracts', 'analyze_result': '.result_analysis', 'compare_analysis_results': '.result_analysis', 'load_analysis_comparison': '.result_analysis', 'load_analysis_request': '.result_analysis', 'load_analysis_result': '.result_analysis', 'write_analysis_comparison': '.result_analysis', 'write_analysis_result': '.result_analysis'}



def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
