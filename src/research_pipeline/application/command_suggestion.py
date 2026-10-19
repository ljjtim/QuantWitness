"""生成可机器执行、也可直接复制到 PowerShell 的后续命令。"""

from __future__ import annotations

import re
from typing import Iterable


_POWERSHELL_SAFE = re.compile(r"^[A-Za-z0-9_./:\\=-]+$")


def render_powershell_command(argv: Iterable[object]) -> str:
    """把 argv 渲染成不依赖当前目录的 PowerShell 单行命令。"""

    values = tuple(str(item) for item in argv)
    if not values or any(not item for item in values):
        raise ValueError("后续命令 argv 不能为空")
    return " ".join(_quote_powershell(item) for item in values)


def command_suggestion(*argv: object) -> dict[str, object]:
    """返回显示命令和机器执行 argv；两者来自同一份参数。"""

    values = [str(item) for item in argv]
    return {
        "next_command": render_powershell_command(values),
        "next_command_argv": values,
    }


def _quote_powershell(value: str) -> str:
    if _POWERSHELL_SAFE.fullmatch(value):
        return value
    return "'" + value.replace("'", "''") + "'"


__all__ = ["command_suggestion", "render_powershell_command"]
