"""首批受信研究变换的纯实现；不读取数据库或动态模块。"""

from __future__ import annotations

from collections.abc import Sequence


def lag_values(values: Sequence[object], *, periods: int) -> tuple[object | None, ...]:
    """按输入顺序产生严格向后位移，不跨越到未来观测。"""

    if type(periods) is not int or periods < 1:
        raise ValueError("periods 必须是正整数")
    prefix = (None,) * min(periods, len(values))
    return (*prefix, *tuple(values)[: max(0, len(values) - periods)])


__all__ = ["lag_values"]
