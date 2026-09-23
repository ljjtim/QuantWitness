"""证据 Facet 共同约束的研究结论。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import EvidenceContractError
from .facets import ArtifactIntegrityFacet, CLAIM_LEVELS, FACET_CONTRACT_VERSION, ResearchValidityFacet, ReproducibilityFacet, require_sha256, strict_fields, weakest_claim_level


@dataclass(frozen=True)
class ClaimFacet:
    claim_level: str
    artifact_integrity_hash: str
    reproducibility_hash: str
    research_validity_hash: str
    limitations: tuple[str, ...]
    claim_hash: str
    contract_version: str = FACET_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.claim_level not in CLAIM_LEVELS:
            raise EvidenceContractError("claim_level 不受支持")
        for field in ("artifact_integrity_hash", "reproducibility_hash", "research_validity_hash", "claim_hash"):
            require_sha256(getattr(self, field), field)
        if any(not isinstance(item, str) or not item.strip() for item in self.limitations):
            raise EvidenceContractError("limitations 必须是非空文本")
        if len(set(self.limitations)) != len(self.limitations):
            raise EvidenceContractError("limitations 不能重复")
        if self.contract_version != FACET_CONTRACT_VERSION or self.claim_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("ClaimFacet hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"claim_level": self.claim_level, "artifact_integrity_hash": self.artifact_integrity_hash, "reproducibility_hash": self.reproducibility_hash, "research_validity_hash": self.research_validity_hash, "limitations": sorted(self.limitations), "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "claim_hash": self.claim_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ClaimFacet":
        strict_fields(payload, {"claim_level", "artifact_integrity_hash", "reproducibility_hash", "research_validity_hash", "limitations", "claim_hash", "contract_version"}, "ClaimFacet")
        limitations = payload["limitations"]
        if not isinstance(limitations, list):
            raise EvidenceContractError("limitations 必须是列表")
        return cls(str(payload["claim_level"]), str(payload["artifact_integrity_hash"]), str(payload["reproducibility_hash"]), str(payload["research_validity_hash"]), tuple(str(item) for item in limitations), str(payload["claim_hash"]), str(payload["contract_version"]))

    @classmethod
    def build(cls, *, requested_level: str, integrity: ArtifactIntegrityFacet, reproducibility: ReproducibilityFacet, validity: ResearchValidityFacet, limitations: tuple[str, ...] = ()) -> "ClaimFacet":
        if requested_level not in CLAIM_LEVELS:
            raise EvidenceContractError("requested_level 不受支持")
        if integrity.status != "pass" or reproducibility.status != "pass":
            ceiling = "research_observation"
        else:
            ceiling = validity.claim_ceiling
        level = weakest_claim_level(requested_level, ceiling)
        normalized_limitations = tuple(sorted(limitations))
        values = (level, integrity.facet_hash, reproducibility.facet_hash, validity.facet_hash, normalized_limitations)
        payload = {"claim_level": level, "artifact_integrity_hash": integrity.facet_hash, "reproducibility_hash": reproducibility.facet_hash, "research_validity_hash": validity.facet_hash, "limitations": list(normalized_limitations), "contract_version": FACET_CONTRACT_VERSION}
        return cls(*values, typed_canonical_hash(payload))


__all__ = ["ClaimFacet"]
