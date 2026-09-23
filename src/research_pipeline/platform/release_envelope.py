"""ReleaseEnvelope、Gate receipt 与验收输入的最小闭包合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping, Sequence

from .build_manifest import BuildManifest


GATE_RECEIPT_VERSION = "research-release-gate-receipt-v2"
RELEASE_ACCEPTANCE_INPUT_VERSION = "research-release-acceptance-input-v2"
RELEASE_ENVELOPE_VERSION = "research-release-envelope-v2"
REQUIRED_GATE_IDS = ("gate-a", "gate-c", "gate-d", "gate-f", "gate-i-b", "gate-l")
LOCAL_SUPPLY_CHAIN_STATUS = MappingProxyType({
    "additional_platforms": "unsupported",
    "artifact_signature": "not_applicable",
    "sbom": "not_applicable",
})


class ReleaseEnvelopeError(ValueError):
    """ReleaseEnvelope 或其前置收据不可信。"""


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ReleaseEnvelopeError(f"{field} 必须是 sha256 小写摘要")
    return value


def _instant(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ReleaseEnvelopeError(f"{field} 必须是带时区时间")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ReleaseEnvelopeError(f"{field} 必须是带时区时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReleaseEnvelopeError(f"{field} 必须是带时区时间")
    return parsed


def _hash_mapping(value: Mapping[str, str], field: str) -> Mapping[str, str]:
    if not value or any(not isinstance(key, str) or not key for key in value):
        raise ReleaseEnvelopeError(f"{field} 必须是非空摘要映射")
    return MappingProxyType({
        key: _hash(item, f"{field}.{key}")
        for key, item in sorted(value.items())
    })


def _gate_ids(values: Sequence[str], field: str) -> tuple[str, ...]:
    normalized = tuple(sorted(values))
    if normalized != REQUIRED_GATE_IDS:
        raise ReleaseEnvelopeError(f"{field} Gate 集合重复或不完整")
    return normalized


@dataclass(frozen=True)
class ReleaseGateReceipt:
    gate_id: str
    release_candidate_id: str
    build_manifest_hash: str
    status: str
    evidence_hashes: Mapping[str, str]
    issued_at: str
    expires_at: str
    contract_version: str = GATE_RECEIPT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != GATE_RECEIPT_VERSION:
            raise ReleaseEnvelopeError("Gate receipt 版本不受支持")
        if self.gate_id not in REQUIRED_GATE_IDS or not self.release_candidate_id:
            raise ReleaseEnvelopeError("Gate receipt gate 或 candidate 身份无效")
        if self.status != "pass":
            raise ReleaseEnvelopeError("Gate receipt 只有 pass 才能进入最终 envelope")
        _hash(self.build_manifest_hash, "build_manifest_hash")
        object.__setattr__(
            self,
            "evidence_hashes",
            _hash_mapping(self.evidence_hashes, "evidence_hashes"),
        )
        if _instant(self.expires_at, "expires_at") <= _instant(self.issued_at, "issued_at"):
            raise ReleaseEnvelopeError("Gate receipt expires_at 必须晚于 issued_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id,
            "release_candidate_id": self.release_candidate_id,
            "build_manifest_hash": self.build_manifest_hash,
            "status": self.status,
            "evidence_hashes": dict(self.evidence_hashes),
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReleaseGateReceipt":
        expected = {
            "gate_id", "release_candidate_id", "build_manifest_hash", "status",
            "evidence_hashes", "issued_at", "expires_at", "contract_version",
        }
        if set(payload) != expected or not isinstance(payload["evidence_hashes"], Mapping):
            raise ReleaseEnvelopeError("Gate receipt schema 无效")
        return cls(
            str(payload["gate_id"]),
            str(payload["release_candidate_id"]),
            str(payload["build_manifest_hash"]),
            str(payload["status"]),
            {str(key): str(value) for key, value in payload["evidence_hashes"].items()},
            str(payload["issued_at"]),
            str(payload["expires_at"]),
            str(payload["contract_version"]),
        )

    @classmethod
    def build(
        cls,
        *,
        gate_id: str,
        release_candidate_id: str,
        build_manifest_hash: str,
        evidence_hashes: Mapping[str, str],
        issued_at: str,
        expires_at: str,
    ) -> "ReleaseGateReceipt":
        return cls(
            gate_id,
            release_candidate_id,
            build_manifest_hash,
            "pass",
            dict(sorted(evidence_hashes.items())),
            issued_at,
            expires_at,
        )


@dataclass(frozen=True)
class ReleaseAcceptanceInput:
    release_candidate_id: str
    build_manifest_hash: str
    gate_ids: tuple[str, ...]
    status: str
    contract_version: str = RELEASE_ACCEPTANCE_INPUT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != RELEASE_ACCEPTANCE_INPUT_VERSION:
            raise ReleaseEnvelopeError("release acceptance input 版本无效")
        if not self.release_candidate_id or self.status != "pass":
            raise ReleaseEnvelopeError("release acceptance input 身份或状态无效")
        _hash(self.build_manifest_hash, "build_manifest_hash")
        object.__setattr__(self, "gate_ids", _gate_ids(self.gate_ids, "release acceptance input"))

    def to_dict(self) -> dict[str, object]:
        return {
            "release_candidate_id": self.release_candidate_id,
            "build_manifest_hash": self.build_manifest_hash,
            "gate_ids": list(self.gate_ids),
            "status": self.status,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReleaseAcceptanceInput":
        expected = {
            "release_candidate_id", "build_manifest_hash", "gate_ids",
            "status", "contract_version",
        }
        if set(payload) != expected or not isinstance(payload["gate_ids"], list):
            raise ReleaseEnvelopeError("release acceptance input schema 无效")
        return cls(
            str(payload["release_candidate_id"]),
            str(payload["build_manifest_hash"]),
            tuple(str(item) for item in payload["gate_ids"]),
            str(payload["status"]),
            str(payload["contract_version"]),
        )

    @classmethod
    def build(
        cls,
        *,
        release_candidate_id: str,
        build_manifest_hash: str,
        receipts: Sequence[ReleaseGateReceipt],
    ) -> "ReleaseAcceptanceInput":
        _require_receipt_set(
            receipts,
            release_candidate_id=release_candidate_id,
            build_manifest_hash=build_manifest_hash,
        )
        return cls(
            release_candidate_id,
            build_manifest_hash,
            tuple(item.gate_id for item in receipts),
            "pass",
        )


@dataclass(frozen=True)
class ReleaseEnvelope:
    release_candidate_id: str
    profile: str
    source_commit: str
    source_tree_digest: str
    wheel_digest: str
    build_manifest_hash: str
    capabilities_digest: str
    dependency_lock_digest: str
    platform: str
    python_cache_tag: str
    gate_ids: tuple[str, ...]
    supply_chain_status: Mapping[str, str]
    issued_at: str
    expires_at: str
    revocation_policy: str
    contract_version: str = RELEASE_ENVELOPE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != RELEASE_ENVELOPE_VERSION:
            raise ReleaseEnvelopeError("ReleaseEnvelope 版本不受支持")
        if not self.release_candidate_id or self.profile != "local":
            raise ReleaseEnvelopeError("ReleaseEnvelope candidate/profile 无效")
        if len(self.source_commit) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in self.source_commit
        ):
            raise ReleaseEnvelopeError("ReleaseEnvelope source_commit 无效")
        for field in (
            "source_tree_digest",
            "wheel_digest",
            "build_manifest_hash",
            "capabilities_digest",
            "dependency_lock_digest",
        ):
            _hash(getattr(self, field), field)
        if not self.platform or not self.python_cache_tag:
            raise ReleaseEnvelopeError("ReleaseEnvelope 平台/ABI 为空")
        object.__setattr__(self, "gate_ids", _gate_ids(self.gate_ids, "ReleaseEnvelope"))
        status = dict(sorted(self.supply_chain_status.items()))
        if set(status) != set(LOCAL_SUPPLY_CHAIN_STATUS):
            raise ReleaseEnvelopeError("ReleaseEnvelope supply_chain_status schema 无效")
        if status != dict(LOCAL_SUPPLY_CHAIN_STATUS):
            raise ReleaseEnvelopeError("local profile 不得冒充 SBOM/签名/额外平台")
        if self.revocation_policy != "not_applicable_no_distribution":
            raise ReleaseEnvelopeError("local profile revocation_policy 无效")
        object.__setattr__(self, "supply_chain_status", MappingProxyType(status))
        if _instant(self.expires_at, "expires_at") <= _instant(self.issued_at, "issued_at"):
            raise ReleaseEnvelopeError("ReleaseEnvelope expires_at 必须晚于 issued_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "release_candidate_id": self.release_candidate_id,
            "profile": self.profile,
            "source_commit": self.source_commit,
            "source_tree_digest": self.source_tree_digest,
            "wheel_digest": self.wheel_digest,
            "build_manifest_hash": self.build_manifest_hash,
            "capabilities_digest": self.capabilities_digest,
            "dependency_lock_digest": self.dependency_lock_digest,
            "platform": self.platform,
            "python_cache_tag": self.python_cache_tag,
            "gate_ids": list(self.gate_ids),
            "supply_chain_status": dict(self.supply_chain_status),
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "revocation_policy": self.revocation_policy,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReleaseEnvelope":
        expected = {
            "release_candidate_id", "profile", "source_commit", "source_tree_digest",
            "wheel_digest", "build_manifest_hash", "capabilities_digest",
            "dependency_lock_digest", "platform", "python_cache_tag", "gate_ids",
            "supply_chain_status", "issued_at", "expires_at", "revocation_policy",
            "contract_version",
        }
        if (
            set(payload) != expected
            or not isinstance(payload["gate_ids"], list)
            or not isinstance(payload["supply_chain_status"], Mapping)
        ):
            raise ReleaseEnvelopeError("ReleaseEnvelope schema 无效")
        return cls(
            str(payload["release_candidate_id"]),
            str(payload["profile"]),
            str(payload["source_commit"]),
            str(payload["source_tree_digest"]),
            str(payload["wheel_digest"]),
            str(payload["build_manifest_hash"]),
            str(payload["capabilities_digest"]),
            str(payload["dependency_lock_digest"]),
            str(payload["platform"]),
            str(payload["python_cache_tag"]),
            tuple(str(item) for item in payload["gate_ids"]),
            {str(key): str(value) for key, value in payload["supply_chain_status"].items()},
            str(payload["issued_at"]),
            str(payload["expires_at"]),
            str(payload["revocation_policy"]),
            str(payload["contract_version"]),
        )

    def require_valid_at(self, as_of: str) -> None:
        instant = _instant(as_of, "as_of")
        if instant < _instant(self.issued_at, "issued_at"):
            raise ReleaseEnvelopeError("ReleaseEnvelope 尚未生效")
        if instant >= _instant(self.expires_at, "expires_at"):
            raise ReleaseEnvelopeError("ReleaseEnvelope 已过期")

    @classmethod
    def build(
        cls,
        *,
        release_candidate_id: str,
        profile: str,
        manifest: BuildManifest,
        capabilities_digest: str,
        dependency_lock_digest: str,
        platform: str,
        python_cache_tag: str,
        receipts: Sequence[ReleaseGateReceipt],
        acceptance: ReleaseAcceptanceInput,
        issued_at: str,
        expires_at: str,
        revocation_policy: str,
        supply_chain_status: Mapping[str, str] | None = None,
    ) -> "ReleaseEnvelope":
        if manifest.source_dirty:
            raise ReleaseEnvelopeError("ReleaseEnvelope 只接受 source_dirty=false 的 BuildManifest")
        _require_receipt_set(
            receipts,
            release_candidate_id=release_candidate_id,
            build_manifest_hash=manifest.manifest_hash,
        )
        actual_gate_ids = tuple(sorted(item.gate_id for item in receipts))
        if (
            acceptance.release_candidate_id != release_candidate_id
            or acceptance.build_manifest_hash != manifest.manifest_hash
            or acceptance.gate_ids != actual_gate_ids
        ):
            raise ReleaseEnvelopeError("release acceptance input 与当前 RC/Gate receipts 不一致")
        return cls(
            release_candidate_id,
            profile,
            manifest.source_commit,
            manifest.source_tree_digest,
            manifest.wheel_digest,
            manifest.manifest_hash,
            capabilities_digest,
            dependency_lock_digest,
            platform,
            python_cache_tag,
            actual_gate_ids,
            dict(supply_chain_status or LOCAL_SUPPLY_CHAIN_STATUS),
            issued_at,
            expires_at,
            revocation_policy,
        )


def _require_receipt_set(
    receipts: Sequence[ReleaseGateReceipt],
    *,
    release_candidate_id: str,
    build_manifest_hash: str,
) -> None:
    by_gate = {item.gate_id: item for item in receipts}
    if len(by_gate) != len(receipts) or tuple(sorted(by_gate)) != REQUIRED_GATE_IDS:
        raise ReleaseEnvelopeError("Gate receipt 集合重复或不完整")
    if any(
        item.release_candidate_id != release_candidate_id
        or item.build_manifest_hash != build_manifest_hash
        for item in receipts
    ):
        raise ReleaseEnvelopeError("旧 candidate/manifest 的 Gate receipt 不可复用")


def verify_release_envelope(
    envelope: ReleaseEnvelope,
    *,
    manifest: BuildManifest,
    receipts: Sequence[ReleaseGateReceipt],
    acceptance: ReleaseAcceptanceInput,
    as_of: str,
) -> None:
    """直接比较小型合同对象并复验完整 ReleaseEnvelope 闭包。"""

    if manifest.source_dirty:
        raise ReleaseEnvelopeError("ReleaseEnvelope 不能绑定 dirty BuildManifest")
    _require_receipt_set(
        receipts,
        release_candidate_id=envelope.release_candidate_id,
        build_manifest_hash=manifest.manifest_hash,
    )
    actual_gate_ids = tuple(sorted(item.gate_id for item in receipts))
    instant = _instant(as_of, "as_of")
    if any(
        instant < _instant(item.issued_at, f"{item.gate_id}.issued_at")
        or instant >= _instant(item.expires_at, f"{item.gate_id}.expires_at")
        for item in receipts
    ):
        raise ReleaseEnvelopeError("ReleaseEnvelope Gate receipt 在验收时刻无效")
    if (
        envelope.source_commit != manifest.source_commit
        or envelope.source_tree_digest != manifest.source_tree_digest
        or envelope.wheel_digest != manifest.wheel_digest
        or envelope.build_manifest_hash != manifest.manifest_hash
        or envelope.gate_ids != actual_gate_ids
        or acceptance.release_candidate_id != envelope.release_candidate_id
        or acceptance.build_manifest_hash != manifest.manifest_hash
        or acceptance.gate_ids != actual_gate_ids
    ):
        raise ReleaseEnvelopeError("ReleaseEnvelope 闭包与实际 RC/收据/acceptance 不一致")
    envelope.require_valid_at(as_of)


__all__ = [
    "GATE_RECEIPT_VERSION",
    "LOCAL_SUPPLY_CHAIN_STATUS",
    "RELEASE_ACCEPTANCE_INPUT_VERSION",
    "RELEASE_ENVELOPE_VERSION",
    "REQUIRED_GATE_IDS",
    "ReleaseAcceptanceInput",
    "ReleaseEnvelope",
    "ReleaseEnvelopeError",
    "ReleaseGateReceipt",
    "verify_release_envelope",
]
