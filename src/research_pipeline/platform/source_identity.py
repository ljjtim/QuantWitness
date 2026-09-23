"""Python 源码身份的跨平台规范化。"""

from __future__ import annotations


def canonical_python_source_bytes(source: bytes) -> bytes:
    """统一 Windows 与 Unix 换行，不改变 Python 源码语义。"""

    return source.replace(b"\r\n", b"\n")


__all__ = ["canonical_python_source_bytes"]
