"""不硬编码资产费率的定点成本、滑点和容量接口。"""

from __future__ import annotations

from typing import Protocol

from research_pipeline.domain import Money, Price


class CostPolicy(Protocol):
    policy_id: str

    def fee(self, *, side: str, price: Price, quantity: int) -> Money: ...


class SlippagePolicy(Protocol):
    policy_id: str

    def execution_price(self, *, side: str, reference: Price, quantity: int) -> Price: ...


class CapacityPolicy(Protocol):
    policy_id: str

    def admitted_quantity(self, *, requested: int, visible_capacity: int) -> int: ...


__all__ = ["CapacityPolicy", "CostPolicy", "SlippagePolicy"]
