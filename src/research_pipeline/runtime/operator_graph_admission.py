"""显式研究复现对照；普通运行准入直接验证正式计划。"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash


STUDY_REPRODUCTION_PROOF_VERSION = "research-study-reproduction-proof-v1"
_STUDY_OUTPUT_FIELDS = (
    "combination_count",
    "simulation_manifest_hash",
    "simulation_semantic_hash",
    "statistics_manifest_hash",
    "statistics_semantic_hash",
    "holdout_terminal_hash",
)


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} 必须是 sha256 小写摘要")
    return value


@dataclass(frozen=True)
class StudyReproductionProof:
    study_id: str
    package_hash: str
    package_plan_hash: str
    expected_outputs: Mapping[str, object]
    retirement_hash: str
    source_proof_hash: str
    proof_hash: str
    contract_version: str = STUDY_REPRODUCTION_PROOF_VERSION

    def __post_init__(self) -> None:
        if not self.study_id:
            raise ValueError("StudyReproductionProof study_id 不能为空")
        for field in ("package_hash", "package_plan_hash", "retirement_hash", "source_proof_hash"):
            _require_hash(getattr(self, field), field)
        if self.contract_version != STUDY_REPRODUCTION_PROOF_VERSION:
            raise ValueError("StudyReproductionProof 版本不受支持")
        if set(self.expected_outputs) != set(_STUDY_OUTPUT_FIELDS):
            raise ValueError("StudyReproductionProof 输出集合无效")
        if type(self.expected_outputs["combination_count"]) is not int or self.expected_outputs["combination_count"] <= 0:
            raise ValueError("StudyReproductionProof combination_count 无效")
        for field in _STUDY_OUTPUT_FIELDS[1:]:
            _require_hash(self.expected_outputs[field], field)
        object.__setattr__(self, "expected_outputs", MappingProxyType(dict(self.expected_outputs)))
        if self.proof_hash != typed_canonical_hash(self.payload()):
            raise ValueError("StudyReproductionProof hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "status": "pass",
            "scope": "study_reproduction",
            "study_id": self.study_id,
            "package_hash": self.package_hash,
            "package_plan_hash": self.package_plan_hash,
            "expected_outputs": dict(self.expected_outputs),
            "retirement_hash": self.retirement_hash,
            "source_proof_hash": self.source_proof_hash,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "proof_hash": self.proof_hash}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StudyReproductionProof":
        expected = {
            "status", "scope", "study_id", "package_hash", "package_plan_hash",
            "expected_outputs", "retirement_hash", "source_proof_hash",
            "contract_version", "proof_hash",
        }
        outputs = payload.get("expected_outputs")
        if (
            set(payload) != expected
            or payload.get("status") != "pass"
            or payload.get("scope") != "study_reproduction"
            or not isinstance(outputs, Mapping)
        ):
            raise ValueError("StudyReproductionProof schema/status 无效")
        return cls(
            str(payload["study_id"]),
            str(payload["package_hash"]),
            str(payload["package_plan_hash"]),
            dict(outputs),
            str(payload["retirement_hash"]),
            str(payload["source_proof_hash"]),
            str(payload["proof_hash"]),
            str(payload["contract_version"]),
        )


def load_study_reproduction_proof(
    path: str | Path,
    *,
    expected_study_id: str,
    expected_package_hash: str,
    expected_package_plan_hash: str,
) -> StudyReproductionProof:
    target = Path(path)
    if not target.is_file():
        raise ValueError("缺少研究复现证明 StudyReproductionProof")
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("研究复现证明无法读取") from exc
    if not isinstance(raw, Mapping):
        raise ValueError("研究复现证明必须是对象")
    proof = StudyReproductionProof.from_dict(raw)
    if (
        proof.study_id != expected_study_id
        or proof.package_hash != expected_package_hash
        or proof.package_plan_hash != expected_package_plan_hash
    ):
        raise ValueError("研究复现证明与 study/package/plan 身份不一致")
    return proof


def verify_study_reproduction_outputs(
    proof: StudyReproductionProof,
    *,
    simulation: Mapping[str, object],
    statistics: Mapping[str, object],
) -> None:
    actual = {
        "combination_count": simulation.get("combination_count"),
        "simulation_manifest_hash": simulation.get("manifest_hash"),
        "simulation_semantic_hash": simulation.get("semantic_hash"),
        "statistics_manifest_hash": statistics.get("statistics_manifest_hash"),
        "statistics_semantic_hash": statistics.get("statistics_semantic_hash"),
        "holdout_terminal_hash": statistics.get("holdout_terminal_hash"),
    }
    mismatches = sorted(field for field, value in actual.items() if proof.expected_outputs[field] != value)
    if mismatches:
        raise ValueError(f"研究复现输出与 StudyReproductionProof 不一致: {','.join(mismatches)}")


__all__ = [
    "STUDY_REPRODUCTION_PROOF_VERSION",
    "StudyReproductionProof",
    "load_study_reproduction_proof",
    "verify_study_reproduction_outputs",
]
