"""中国期货逐日盯市、保证金和确定性强平计算。"""

from __future__ import annotations

from dataclasses import dataclass

from .orders import SimulationContractError


@dataclass(frozen=True)
class MarginPolicy:
    policy_id: str
    initial_margin_ppm: int
    maintenance_margin_ppm: int
    broker_addon_ppm: int = 0
    offset_policy_id: str | None = None

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or not 0 < self.maintenance_margin_ppm <= self.initial_margin_ppm <= 1_000_000:
            raise SimulationContractError("保证金比例无效")
        if self.broker_addon_ppm < 0:
            raise SimulationContractError("券商保证金加收不能为负")

    def required_units(self, *, price_units: int, multiplier: int, contracts: int) -> int:
        gross = abs(contracts) * price_units * multiplier
        rate = self.initial_margin_ppm + self.broker_addon_ppm
        return (gross * rate + 999_999) // 1_000_000

    def maintenance_units(self, *, price_units: int, multiplier: int, contracts: int) -> int:
        gross = abs(contracts) * price_units * multiplier
        return (gross * self.maintenance_margin_ppm + 999_999) // 1_000_000


def deterministic_liquidation_order(positions: tuple[tuple[str, int, int], ...], priority: tuple[str, ...]) -> tuple[str, ...]:
    rank = {code: index for index, code in enumerate(priority)}
    active = (item for item in positions if item[1] != 0)
    return tuple(item[0] for item in sorted(active, key=lambda item: (rank.get(item[0], len(rank)), -abs(item[1]) * item[2], item[0])))


__all__ = ["MarginPolicy", "deterministic_liquidation_order"]
