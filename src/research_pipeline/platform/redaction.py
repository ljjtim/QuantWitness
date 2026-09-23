"""跨层可复用的最小文本脱敏。"""

from __future__ import annotations

import re


_PEM = re.compile(
    r"-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----",
    re.I | re.S,
)
_URI_USERINFO = re.compile(r"([a-z][a-z0-9+.-]*://)[^/@\s]+@", re.I)
_TOKEN = re.compile(
    r"(?i)(bearer\s+)[a-z0-9._~+/-]+|"
    r"((?:token|cookie|password)\s*[=:]\s*)[^\s,;]+"
)


def redact_text(value: str) -> str:
    """移除日志和公开错误摘要中的常见凭据正文。"""

    value = _PEM.sub("<redacted>", value)
    value = _URI_USERINFO.sub(r"\1<redacted>@", value)
    return _TOKEN.sub(
        lambda match: (match.group(1) or match.group(2) or "") + "<redacted>",
        value,
    )


__all__ = ["redact_text"]
