"""列式数据平面的稳定错误类型。"""

from __future__ import annotations

from typing import Mapping

from research_pipeline.platform.errors import MainlineError


class DataPlaneError(MainlineError):
    """列式数据平面错误基类。"""

    error_code = "data_plane_error"


class QueryIRInvalidError(DataPlaneError):
    error_code = "query_ir_invalid"


class QueryUnboundedError(DataPlaneError):
    error_code = "query_unbounded"


class QueryCatalogUnapprovedError(DataPlaneError):
    error_code = "query_catalog_unapproved"


class QueryAttestationStaleError(DataPlaneError):
    error_code = "query_attestation_stale"


class ProviderExecutionError(DataPlaneError):
    error_code = "provider_execution_invalid"


class DataPlaneRequestExecutionError(ProviderExecutionError):
    """把当前 request 的安全定位事实交给既有 Runtime diagnostic 事件。"""

    def __init__(
        self,
        message: str,
        *,
        failure_payload: Mapping[str, object],
    ) -> None:
        super().__init__(message)
        expected = {
            "contract_version",
            "request_status",
            "request_id",
            "dataset_id",
            "binding_id",
            "object_name",
            "provider",
            "output_budget",
            "execution_budget",
            "completed_request_ids",
            "underlying_exception_type",
        }
        if set(failure_payload) != expected:
            raise ValueError("data-plane request failure payload schema 无效")
        self.failure_payload = dict(failure_payload)


class SourceChangedError(DataPlaneError):
    error_code = "source_changed"


class SnapshotIntegrityError(DataPlaneError):
    error_code = "snapshot_integrity_invalid"


class QualityGateError(DataPlaneError):
    error_code = "quality_gate_failed"


__all__ = [
    "DataPlaneError",
    "DataPlaneRequestExecutionError",
    "ProviderExecutionError",
    "QualityGateError",
    "QueryAttestationStaleError",
    "QueryCatalogUnapprovedError",
    "QueryIRInvalidError",
    "QueryUnboundedError",
    "SnapshotIntegrityError",
    "SourceChangedError",
]
