"""完整性、可复现性和研究有效性三个独立证据 Facet。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.claim_levels import (
    CLAIM_LEVELS,
    weakest_claim_level as _weakest_claim_level,
)

from .errors import EvidenceContractError


FACET_CONTRACT_VERSION = "research-evidence-facets-v1"
FACET_STATUSES = {"pass", "fail"}
ISSUE_LAYERS = {"structure", "artifact", "lineage", "reproducibility", "validity", "trust"}
_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")


def require_sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise EvidenceContractError(f"{field} 必须是 sha256 小写十六进制")
    return value


def require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _STABLE_ID.fullmatch(value):
        raise EvidenceContractError(f"{field} 必须是稳定 ID")
    return value


def strict_fields(payload: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise EvidenceContractError(f"{label} 字段不匹配: missing={sorted(expected-actual)}, unknown={sorted(actual-expected)}")


@dataclass(frozen=True)
class VerificationIssue:
    layer: str
    code: str
    message: str
    object_type: str
    object_id: str

    def __post_init__(self) -> None:
        if self.layer not in ISSUE_LAYERS:
            raise EvidenceContractError("issue layer 不受支持")
        require_id(self.code, "issue code")
        require_id(self.object_type, "object_type")
        require_id(self.object_id, "object_id")
        if not isinstance(self.message, str) or not self.message.strip():
            raise EvidenceContractError("issue message 不能为空")

    def to_dict(self) -> dict[str, str]:
        return {"layer": self.layer, "code": self.code, "message": self.message, "object_type": self.object_type, "object_id": self.object_id}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "VerificationIssue":
        strict_fields(payload, {"layer", "code", "message", "object_type", "object_id"}, "VerificationIssue")
        return cls(*(str(payload[key]) for key in ("layer", "code", "message", "object_type", "object_id")))


def _issues_payload(issues: tuple[VerificationIssue, ...]) -> list[dict[str, str]]:
    return [item.to_dict() for item in _sorted_issues(issues)]


def _sorted_issues(issues: tuple[VerificationIssue, ...]) -> tuple[VerificationIssue, ...]:
    return tuple(sorted(issues, key=lambda item: (item.layer, item.code, item.object_type, item.object_id)))


def _validate_status(status: str, issues: tuple[VerificationIssue, ...]) -> None:
    if status not in FACET_STATUSES:
        raise EvidenceContractError("facet status 只能是 pass/fail")
    if (status == "pass") == bool(issues):
        raise EvidenceContractError("pass 不能有 issue，fail 必须有 issue")


@dataclass(frozen=True)
class ArtifactIntegrityFacet:
    artifact_catalog_hash: str
    status: str
    issues: tuple[VerificationIssue, ...]
    facet_hash: str
    contract_version: str = FACET_CONTRACT_VERSION

    def __post_init__(self) -> None:
        require_sha256(self.artifact_catalog_hash, "artifact_catalog_hash")
        _validate_status(self.status, self.issues)
        if self.contract_version != FACET_CONTRACT_VERSION or self.facet_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("ArtifactIntegrityFacet hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"artifact_catalog_hash": self.artifact_catalog_hash, "status": self.status, "issues": _issues_payload(self.issues), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "facet_hash": self.facet_hash}

    @classmethod
    def build(cls, artifact_catalog_hash: str, issues: tuple[VerificationIssue, ...] = ()) -> "ArtifactIntegrityFacet":
        normalized_issues = _sorted_issues(issues)
        payload = {"artifact_catalog_hash": artifact_catalog_hash, "status": "fail" if normalized_issues else "pass", "issues": _issues_payload(normalized_issues), "contract_version": FACET_CONTRACT_VERSION}
        return cls(artifact_catalog_hash, str(payload["status"]), normalized_issues, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ArtifactIntegrityFacet":
        strict_fields(payload, {"artifact_catalog_hash", "status", "issues", "facet_hash", "contract_version"}, "ArtifactIntegrityFacet")
        return cls(str(payload["artifact_catalog_hash"]), str(payload["status"]), tuple(VerificationIssue.from_dict(item) for item in _mapping_list(payload["issues"])), str(payload["facet_hash"]), str(payload["contract_version"]))


@dataclass(frozen=True)
class ReproducibilityFacet:
    plan_hash: str
    snapshot_hashes: tuple[str, ...]
    lineage_hash: str
    runtime_hash: str
    implementation_manifest_hash: str
    deterministic: bool
    status: str
    issues: tuple[VerificationIssue, ...]
    facet_hash: str
    contract_version: str = FACET_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for field in ("plan_hash", "lineage_hash", "runtime_hash", "implementation_manifest_hash"):
            require_sha256(getattr(self, field), field)
        if not self.snapshot_hashes or len(set(self.snapshot_hashes)) != len(self.snapshot_hashes):
            raise EvidenceContractError("snapshot_hashes 必须非空且不重复")
        for value in self.snapshot_hashes:
            require_sha256(value, "snapshot_hash")
        if type(self.deterministic) is not bool:
            raise EvidenceContractError("deterministic 必须是布尔值")
        _validate_status(self.status, self.issues)
        if self.contract_version != FACET_CONTRACT_VERSION or self.facet_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("ReproducibilityFacet hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"plan_hash": self.plan_hash, "snapshot_hashes": sorted(self.snapshot_hashes), "lineage_hash": self.lineage_hash, "runtime_hash": self.runtime_hash, "implementation_manifest_hash": self.implementation_manifest_hash, "deterministic": self.deterministic, "status": self.status, "issues": _issues_payload(self.issues), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "facet_hash": self.facet_hash}

    @classmethod
    def build(cls, *, plan_hash: str, snapshot_hashes: tuple[str, ...], lineage_hash: str, runtime_hash: str, implementation_manifest_hash: str, deterministic: bool, issues: tuple[VerificationIssue, ...] = ()) -> "ReproducibilityFacet":
        normalized_snapshots = tuple(sorted(snapshot_hashes))
        normalized_issues = _sorted_issues(issues)
        values = (plan_hash, normalized_snapshots, lineage_hash, runtime_hash, implementation_manifest_hash, deterministic, "fail" if normalized_issues else "pass", normalized_issues)
        payload = {"plan_hash": plan_hash, "snapshot_hashes": list(normalized_snapshots), "lineage_hash": lineage_hash, "runtime_hash": runtime_hash, "implementation_manifest_hash": implementation_manifest_hash, "deterministic": deterministic, "status": values[6], "issues": _issues_payload(normalized_issues), "contract_version": FACET_CONTRACT_VERSION}
        return cls(*values, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReproducibilityFacet":
        expected = {"plan_hash", "snapshot_hashes", "lineage_hash", "runtime_hash", "implementation_manifest_hash", "deterministic", "status", "issues", "facet_hash", "contract_version"}
        strict_fields(payload, expected, "ReproducibilityFacet")
        return cls(str(payload["plan_hash"]), tuple(str(v) for v in _list(payload["snapshot_hashes"])), str(payload["lineage_hash"]), str(payload["runtime_hash"]), str(payload["implementation_manifest_hash"]), _bool(payload["deterministic"]), str(payload["status"]), tuple(VerificationIssue.from_dict(item) for item in _mapping_list(payload["issues"])), str(payload["facet_hash"]), str(payload["contract_version"]))


def weakest_claim_level(*levels: str) -> str:
    """返回一组正式结论等级中最保守的一个。"""

    try:
        return _weakest_claim_level(*levels)
    except ValueError as exc:
        raise EvidenceContractError("claim level 集合无效") from exc


@dataclass(frozen=True)
class ResearchValidityFacet:
    gate_result_hashes: tuple[str, ...]
    claim_ceiling: str
    status: str
    issues: tuple[VerificationIssue, ...]
    facet_hash: str
    contract_version: str = FACET_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if not self.gate_result_hashes or len(set(self.gate_result_hashes)) != len(self.gate_result_hashes):
            raise EvidenceContractError("gate_result_hashes 必须非空且不重复")
        for value in self.gate_result_hashes:
            require_sha256(value, "gate_result_hash")
        if self.claim_ceiling not in CLAIM_LEVELS:
            raise EvidenceContractError("claim_ceiling 不受支持")
        _validate_status(self.status, self.issues)
        if self.status == "fail" and self.claim_ceiling != "research_observation":
            raise EvidenceContractError("validity fail 的 ceiling 只能是 research_observation")
        if self.contract_version != FACET_CONTRACT_VERSION or self.facet_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("ResearchValidityFacet hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"gate_result_hashes": sorted(self.gate_result_hashes), "claim_ceiling": self.claim_ceiling, "status": self.status, "issues": _issues_payload(self.issues), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "facet_hash": self.facet_hash}

    @classmethod
    def build(cls, gate_result_hashes: tuple[str, ...], claim_ceiling: str, issues: tuple[VerificationIssue, ...] = ()) -> "ResearchValidityFacet":
        normalized_gates = tuple(sorted(gate_result_hashes))
        normalized_issues = _sorted_issues(issues)
        values = (normalized_gates, claim_ceiling, "fail" if normalized_issues else "pass", normalized_issues)
        payload = {"gate_result_hashes": list(normalized_gates), "claim_ceiling": claim_ceiling, "status": values[2], "issues": _issues_payload(normalized_issues), "contract_version": FACET_CONTRACT_VERSION}
        return cls(*values, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResearchValidityFacet":
        strict_fields(payload, {"gate_result_hashes", "claim_ceiling", "status", "issues", "facet_hash", "contract_version"}, "ResearchValidityFacet")
        return cls(tuple(str(v) for v in _list(payload["gate_result_hashes"])), str(payload["claim_ceiling"]), str(payload["status"]), tuple(VerificationIssue.from_dict(item) for item in _mapping_list(payload["issues"])), str(payload["facet_hash"]), str(payload["contract_version"]))


def _list(value: object) -> list[Any]:
    if not isinstance(value, list):
        raise EvidenceContractError("codec 字段必须是列表")
    return value


def _mapping_list(value: object) -> list[Mapping[str, object]]:
    items = _list(value)
    if any(not isinstance(item, Mapping) for item in items):
        raise EvidenceContractError("issue 列表元素必须是对象")
    return [item for item in items if isinstance(item, Mapping)]


def _bool(value: object) -> bool:
    if type(value) is not bool:
        raise EvidenceContractError("codec 布尔字段类型错误")
    return value


__all__ = ["ArtifactIntegrityFacet", "CLAIM_LEVELS", "FACET_CONTRACT_VERSION", "ResearchValidityFacet", "ReproducibilityFacet", "VerificationIssue", "require_id", "require_sha256", "strict_fields", "weakest_claim_level"]
