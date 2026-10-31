"""执行与独立验证共用的不可变资源预算。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import RuntimeContractError


@dataclass(frozen=True)
class ResourceBudget:
    memory_bytes: int
    cpu_slots: int
    temp_bytes: int
    wall_seconds: int  # 有效运行预算中，0表示不限制节点累计时长。

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            minimum = 0 if name in {"temp_bytes", "wall_seconds"} else 1
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                requirement = "非负整数" if name in {"temp_bytes", "wall_seconds"} else "正整数"
                raise RuntimeContractError(f"资源预算 {name} 必须是{requirement}")

    def to_dict(self) -> dict[str, int]:
        return {
            "memory_bytes": self.memory_bytes,
            "cpu_slots": self.cpu_slots,
            "temp_bytes": self.temp_bytes,
            "wall_seconds": self.wall_seconds,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ResourceBudget:
        fields = ("memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds")
        unknown, missing = set(payload) - set(fields), set(fields) - set(payload)
        if unknown:
            raise RuntimeContractError(f"ResourceBudget 含未知字段: {sorted(unknown)}")
        if missing:
            raise RuntimeContractError(f"ResourceBudget 缺少字段: {sorted(missing)}")
        return cls(*(payload[key] for key in fields))


