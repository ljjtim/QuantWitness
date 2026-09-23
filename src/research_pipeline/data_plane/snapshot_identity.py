"""逻辑快照身份；与物理布局解耦。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.platform.canonical import typed_canonical_hash

from .admission import AdmittedQueryPlan
from .errors import SnapshotIntegrityError
from .revision import SourceRevision


LOGICAL_SNAPSHOT_VERSION = "logical-snapshot-v1"


@dataclass(frozen=True)
class LogicalSnapshot:
    admitted_plan_hash: str
    source_revisions: tuple[SourceRevision, ...]
    output_schema: str
    provider_version: str
    compiler_version: str
    contract_version: str = LOGICAL_SNAPSHOT_VERSION

    @property
    def logical_snapshot_id(self) -> str:
        return typed_canonical_hash(self.identity_payload())

    def identity_payload(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "admitted_plan_hash": self.admitted_plan_hash,
            "source_revisions": [
                item.to_dict()
                for item in sorted(self.source_revisions, key=lambda value: value.source_id)
            ],
            "output_schema": self.output_schema,
            "provider_version": self.provider_version,
            "compiler_version": self.compiler_version,
        }


def build_logical_snapshot(
    plan: AdmittedQueryPlan,
    *,
    source_revisions: tuple[SourceRevision, ...],
    output_schema: object,
    provider_version: str,
    compiler_version: str,
) -> LogicalSnapshot:
    if not source_revisions or not provider_version or not compiler_version:
        raise SnapshotIntegrityError("逻辑快照缺少来源或实现版本")
    return LogicalSnapshot(
        plan.plan_hash,
        tuple(sorted(source_revisions, key=lambda item: item.source_id)),
        str(output_schema),
        provider_version,
        compiler_version,
    )


__all__ = ["LOGICAL_SNAPSHOT_VERSION", "LogicalSnapshot", "build_logical_snapshot"]
