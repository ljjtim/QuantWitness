"""Python 源码身份的跨平台规范化与语义 token 规范化。"""

from __future__ import annotations

import io
import tokenize


def canonical_python_source_bytes(source: bytes) -> bytes:
    """统一 Windows 与 Unix 换行，不改变 Python 源码语义。"""

    return source.replace(b"\r\n", b"\n")


_IGNORED_SEMANTIC_TOKENS = frozenset({
    tokenize.ENCODING,
    tokenize.COMMENT,
    tokenize.NL,
    tokenize.ENDMARKER,
})
_STRUCTURAL_NEWLINE_TOKENS = frozenset({
    tokenize.INDENT,
    tokenize.DEDENT,
    tokenize.NEWLINE,
})


def semantic_python_source_bytes(source: bytes) -> bytes:
    """剥离注释与无关空白，只保留 Python 词法结构与代码 token。"""

    canonical = canonical_python_source_bytes(source)
    parts: list[str] = []
    try:
        tokens = list(tokenize.tokenize(io.BytesIO(canonical).readline))
    except (tokenize.TokenError, IndentationError):
        return canonical
    for tok in tokens:
        if tok.type in _IGNORED_SEMANTIC_TOKENS:
            continue
        if tok.type in _STRUCTURAL_NEWLINE_TOKENS:
            parts.append(f"{tok.type}:")
        else:
            parts.append(f"{tok.type}:{tok.string}")
    return "\n".join(parts).encode("utf-8")


__all__ = ["canonical_python_source_bytes", "semantic_python_source_bytes"]
