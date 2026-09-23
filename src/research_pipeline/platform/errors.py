"""新研究主链的稳定错误分类。"""

from __future__ import annotations


class MainlineError(ValueError):
    """可在库层保留具体类型、在 CLI 层稳定分类的主链错误。"""

    error_code = "mainline_error"


class CanonicalEncodingError(MainlineError):
    """输入无法按已声明的 canonical codec 无歧义编码。"""

    error_code = "canonical_encoding_invalid"


def error_code_for_exception(error: BaseException) -> str:
    """返回稳定机器码；异常文本只供人阅读，不参与机器分支。"""
    if isinstance(error, MainlineError):
        return error.error_code
    if isinstance(error, OSError):
        return "io_error"
    if isinstance(error, TypeError):
        return "type_error"
    if isinstance(error, ValueError):
        return "value_error"
    return "internal_error"


__all__ = [
    "CanonicalEncodingError",
    "MainlineError",
    "error_code_for_exception",
]
