"""只读列式 Provider 协议。"""

from __future__ import annotations

from typing import Protocol

from ..admission import AdmittedQueryPlan
from ..execution_budget import DataPlaneExecutionBudget
from ..execution_estimate import ExecutionEstimate
from ..stream import ColumnarStream


class ReadOnlyColumnarProvider(Protocol):
    def open_stream(
        self,
        plan: AdmittedQueryPlan,
        *,
        execution_budget: DataPlaneExecutionBudget,
        execution_estimate: ExecutionEstimate,
        scratch_root: str,
    ) -> ColumnarStream: ...


__all__ = ["ReadOnlyColumnarProvider"]
