"""正式结果的证据异常接口。"""
from importlib import import_module

__all__ = ['EvidenceContractError']

_LAZY_EXPORTS = {'EvidenceContractError': '.errors'}



def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
