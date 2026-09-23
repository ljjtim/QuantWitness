"""统一运行时的稳定错误分类。"""

from __future__ import annotations

from typing import Mapping

from research_pipeline.platform.errors import MainlineError


class RuntimeErrorBase(MainlineError):
    error_code = "runtime_error"


class RuntimeContractError(RuntimeErrorBase):
    error_code = "runtime_contract_invalid"


class RuntimeGraphError(RuntimeContractError):
    error_code = "runtime_graph_invalid"


class RuntimeRegistryError(RuntimeContractError):
    error_code = "runtime_registry_invalid"


class RuntimeStateError(RuntimeErrorBase):
    error_code = "runtime_state_invalid"


class RuntimeIntegrityError(RuntimeErrorBase):
    error_code = "runtime_integrity_invalid"


class RuntimeAdmissionError(RuntimeErrorBase):
    error_code = "runtime_resource_admission"


class RuntimeWorkerError(RuntimeErrorBase):
    error_code = "runtime_worker_failed"

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        failure_payload: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        if error_code is not None:
            self.error_code = error_code
        if failure_payload is not None:
            self.failure_payload = dict(failure_payload)


__all__ = [
    "RuntimeAdmissionError",
    "RuntimeContractError",
    "RuntimeErrorBase",
    "RuntimeGraphError",
    "RuntimeIntegrityError",
    "RuntimeRegistryError",
    "RuntimeStateError",
    "RuntimeWorkerError",
]
