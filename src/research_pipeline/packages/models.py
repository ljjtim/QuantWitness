"""研究包声明及其稳定身份。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlparse

from research_pipeline.evidence.facets import CLAIM_LEVELS
from research_pipeline.platform import MainlineError, typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    normalize_asset_class,
)


RESEARCH_PACKAGE_VERSION = "research-package-v1"
SOURCE_PROVENANCE_VERSION = "research-source-provenance-v1"
SOURCE_PROVENANCE_MODES = ("citation_only", "archived_snapshot")
_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")
_FILE_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
_MEDIA_TYPE = re.compile(r"^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$")


class ResearchPackageError(MainlineError):
    """研究包声明、来源、本土化或交付合同无效。"""

    error_code = "research_package_invalid"


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchPackageError(f"{field} 必须是非空字符串")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ResearchPackageError(f"{field} 必须是 sha256 小写摘要")
    return value


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ResearchPackageError("spec_payload 的 mapping key 必须是字符串")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise ResearchPackageError(f"spec_payload 不支持类型: {type(value).__name__}")


@dataclass(frozen=True)
class SourceProvenance:
    """来源正文是否具备可核验的离线快照。"""

    mode: str
    content_digest: str | None = None
    media_type: str | None = None
    snapshot_artifact_id: str | None = None
    snapshot_manifest_hash: str | None = None
    importer_id: str | None = None
    imported_at: str | None = None
    contract_version: str = SOURCE_PROVENANCE_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != SOURCE_PROVENANCE_VERSION:
            raise ResearchPackageError("source provenance contract version 不受支持")
        if self.mode not in SOURCE_PROVENANCE_MODES:
            raise ResearchPackageError("source provenance mode 不受支持")
        snapshot_values = (
            self.content_digest,
            self.media_type,
            self.snapshot_artifact_id,
            self.snapshot_manifest_hash,
            self.importer_id,
            self.imported_at,
        )
        if self.mode == "citation_only":
            if any(value is not None for value in snapshot_values):
                raise ResearchPackageError("citation_only 不得伪装成已归档正文")
            return
        if any(value is None for value in snapshot_values):
            raise ResearchPackageError("archived_snapshot 必须完整声明离线快照")
        _require_hash(self.content_digest, "source.provenance.content_digest")
        _require_hash(self.snapshot_manifest_hash, "source.provenance.snapshot_manifest_hash")
        if not isinstance(self.snapshot_artifact_id, str) or not _FILE_ID.fullmatch(self.snapshot_artifact_id):
            raise ResearchPackageError("source provenance snapshot_artifact_id 必须是安全文件 ID")
        if not isinstance(self.media_type, str) or not _MEDIA_TYPE.fullmatch(self.media_type):
            raise ResearchPackageError("source provenance media_type 必须是规范 MIME 类型")
        if not isinstance(self.importer_id, str) or not _STABLE_ID.fullmatch(self.importer_id):
            raise ResearchPackageError("source provenance importer_id 必须是稳定 ID")
        try:
            imported = datetime.fromisoformat(str(self.imported_at))
        except ValueError as exc:
            raise ResearchPackageError("source provenance imported_at 必须是 ISO 时间") from exc
        if imported.tzinfo is None or imported.utcoffset() is None:
            raise ResearchPackageError("source provenance imported_at 必须带时区")

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "content_digest": self.content_digest,
            "media_type": self.media_type,
            "snapshot_artifact_id": self.snapshot_artifact_id,
            "snapshot_manifest_hash": self.snapshot_manifest_hash,
            "importer_id": self.importer_id,
            "imported_at": self.imported_at,
            "contract_version": self.contract_version,
        }

    @classmethod
    def citation_only(cls) -> "SourceProvenance":
        return cls("citation_only")


@dataclass(frozen=True)
class PackageSource:
    source_id: str
    source_type: str
    title: str
    url: str
    accessed_at: str
    content_hash: str
    license_id: str
    status: str
    limitation: str | None = None
    provenance: SourceProvenance = SourceProvenance(mode="citation_only")

    def __post_init__(self) -> None:
        for field in ("source_id", "source_type", "title", "url", "accessed_at", "content_hash", "license_id", "status"):
            _require_text(getattr(self, field), field)
        if self.source_type not in {"paper", "code", "dataset", "method"}:
            raise ResearchPackageError("source_type 不受支持")
        parsed = urlparse(self.url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ResearchPackageError("source url 必须是 https")
        _require_hash(self.content_hash, "source.content_hash")
        try:
            date.fromisoformat(self.accessed_at)
        except ValueError as exc:
            raise ResearchPackageError("source.accessed_at 必须是 ISO 日期") from exc
        if self.status not in {"available", "unavailable"}:
            raise ResearchPackageError("source.status 不受支持")
        if self.status == "unavailable":
            _require_text(self.limitation, "unavailable source limitation")
        if self.provenance.mode == "archived_snapshot":
            if self.status != "available":
                raise ResearchPackageError("unavailable source 不能声明已归档正文")
            if self.content_hash != self.provenance.content_digest:
                raise ResearchPackageError("archived_snapshot 的 content_hash 必须来自真实正文摘要")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "title": self.title,
            "url": self.url,
            "accessed_at": self.accessed_at,
            "content_hash": self.content_hash,
            "license_id": self.license_id,
            "status": self.status,
            "limitation": self.limitation,
            "provenance": self.provenance.to_dict(),
        }


@dataclass(frozen=True)
class LocalizationDecision:
    decision_id: str
    original_assumption: str
    local_market: str
    local_adaptation: str
    evidence_source_ids: tuple[str, ...]
    status: str
    claim_effect: str

    def __post_init__(self) -> None:
        for field in ("decision_id", "original_assumption", "local_market", "local_adaptation", "status", "claim_effect"):
            _require_text(getattr(self, field), field)
        try:
            object.__setattr__(
                self,
                "local_market",
                normalize_asset_class(self.local_market, field="local_market"),
            )
        except AssetTaxonomyError as exc:
            raise ResearchPackageError(str(exc)) from exc
        if self.status not in {"adopted", "proxy", "unavailable"}:
            raise ResearchPackageError("localization status 不受支持")
        if self.claim_effect not in {"none", "downgrade_to_research_observation", "not_sealable"}:
            raise ResearchPackageError("localization claim_effect 不受支持")
        if not self.evidence_source_ids or len(self.evidence_source_ids) != len(set(self.evidence_source_ids)):
            raise ResearchPackageError("localization evidence_source_ids 必须非空且唯一")
        if self.status != "adopted" and self.claim_effect == "none":
            raise ResearchPackageError("proxy/unavailable 必须降低 claim 或禁止封存")

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "original_assumption": self.original_assumption,
            "local_market": self.local_market,
            "local_adaptation": self.local_adaptation,
            "evidence_source_ids": list(self.evidence_source_ids),
            "status": self.status,
            "claim_effect": self.claim_effect,
        }


@dataclass(frozen=True)
class MetricContract:
    contract_id: str
    version: str
    metrics: tuple[str, ...]
    semantics: Mapping[str, str]
    contract_hash: str

    def __post_init__(self) -> None:
        _require_text(self.contract_id, "metric contract_id")
        _require_text(self.version, "metric version")
        if not self.metrics or len(self.metrics) != len(set(self.metrics)):
            raise ResearchPackageError("metrics 必须非空且唯一")
        if set(self.semantics) != set(self.metrics):
            raise ResearchPackageError("metric semantics 必须覆盖全部且仅覆盖声明指标")
        for metric in self.metrics:
            _require_text(metric, "metric")
            _require_text(self.semantics[metric], f"metric semantics.{metric}")
        object.__setattr__(self, "semantics", MappingProxyType(dict(self.semantics)))
        if self.contract_hash != typed_canonical_hash(self.payload()):
            raise ResearchPackageError("metric contract hash 不一致")

    def payload(self) -> dict[str, object]:
        return {"contract_id": self.contract_id, "version": self.version, "metrics": list(self.metrics), "semantics": dict(self.semantics)}

    @classmethod
    def build(cls, *, contract_id: str, version: str, metrics: tuple[str, ...], semantics: Mapping[str, str]) -> "MetricContract":
        payload = {"contract_id": contract_id, "version": version, "metrics": list(metrics), "semantics": dict(semantics)}
        return cls(contract_id, version, metrics, semantics, typed_canonical_hash(payload))


@dataclass(frozen=True)
class PackageClaimContract:
    allowed_claim_levels: tuple[str, ...]
    max_claim_level: str
    contract_hash: str

    def __post_init__(self) -> None:
        if not self.allowed_claim_levels or len(self.allowed_claim_levels) != len(set(self.allowed_claim_levels)) or any(item not in CLAIM_LEVELS for item in self.allowed_claim_levels):
            raise ResearchPackageError("allowed_claim_levels 无效")
        if self.max_claim_level not in self.allowed_claim_levels:
            raise ResearchPackageError("max_claim_level 必须在 allowed_claim_levels")
        maximum = CLAIM_LEVELS.index(self.max_claim_level)
        if any(CLAIM_LEVELS.index(item) > maximum for item in self.allowed_claim_levels):
            raise ResearchPackageError("allowed_claim_levels 不能超过 max_claim_level")
        if self.contract_hash != typed_canonical_hash(self.payload()):
            raise ResearchPackageError("claim contract hash 不一致")

    def payload(self) -> dict[str, object]:
        return {"allowed_claim_levels": list(self.allowed_claim_levels), "max_claim_level": self.max_claim_level}

    @classmethod
    def build(cls, *, allowed_claim_levels: tuple[str, ...], max_claim_level: str) -> "PackageClaimContract":
        payload = {"allowed_claim_levels": list(allowed_claim_levels), "max_claim_level": max_claim_level}
        return cls(allowed_claim_levels, max_claim_level, typed_canonical_hash(payload))


@dataclass(frozen=True)
class ResearchPackage:
    package_id: str
    package_hash: str
    package_slug: str
    display_name: str
    package_version: str
    builder_id: str
    sources: tuple[PackageSource, ...]
    localizations: tuple[LocalizationDecision, ...]
    metric_contract: MetricContract
    claim_contract: PackageClaimContract
    spec_payload: Mapping[str, object]
    contract_version: str = RESEARCH_PACKAGE_VERSION

    def __post_init__(self) -> None:
        for field in ("package_id", "package_hash", "package_slug", "display_name", "package_version", "builder_id"):
            _require_text(getattr(self, field), field)
        _require_hash(self.package_hash, "package_hash")
        if not self.sources or not any(item.status == "available" for item in self.sources):
            raise ResearchPackageError("package 至少需要一个 available source")
        source_ids = tuple(item.source_id for item in self.sources)
        if len(source_ids) != len(set(source_ids)):
            raise ResearchPackageError("source_id 不能重复")
        if not self.localizations:
            raise ResearchPackageError("package 至少需要一条 localization")
        if len({item.decision_id for item in self.localizations}) != len(self.localizations):
            raise ResearchPackageError("localization decision_id 不能重复")
        for decision in self.localizations:
            if not set(decision.evidence_source_ids).issubset(source_ids):
                raise ResearchPackageError("localization 引用了未声明 source")
            if decision.claim_effect != "none" and self.claim_contract.max_claim_level != "research_observation":
                raise ResearchPackageError("本土化降级未反映到 package claim 上限")
        object.__setattr__(self, "spec_payload", _freeze(self.spec_payload))
        if self.contract_version != RESEARCH_PACKAGE_VERSION:
            raise ResearchPackageError("research package contract version 不受支持")
        if self.package_hash != typed_canonical_hash(self.payload()):
            raise ResearchPackageError("package_hash 与声明内容不一致")
        if self.package_id != f"research_package_{self.package_hash[:16]}":
            raise ResearchPackageError("package_id 必须由 package_hash 派生")

    def payload(self) -> dict[str, object]:
        return {
            "package_slug": self.package_slug,
            "display_name": self.display_name,
            "package_version": self.package_version,
            "builder_id": self.builder_id,
            "sources": [item.to_dict() for item in self.sources],
            "localizations": [item.to_dict() for item in self.localizations],
            "metric_contract": {"contract_hash": self.metric_contract.contract_hash, **self.metric_contract.payload()},
            "claim_contract": {"contract_hash": self.claim_contract.contract_hash, **self.claim_contract.payload()},
            "spec_payload": dict(self.spec_payload),
            "contract_version": self.contract_version,
        }

    def research_identity_payload(self, *, plan_hash: str) -> dict[str, object]:
        _require_hash(plan_hash, "plan_hash")
        return {
            "package_id": self.package_id,
            "package_hash": self.package_hash,
            "plan_hash": plan_hash,
            "metric_contract_hash": self.metric_contract.contract_hash,
            "claim_contract_hash": self.claim_contract.contract_hash,
            "source_provenance_hash": self.source_provenance_hash,
            "contract_version": self.contract_version,
        }

    @property
    def source_provenance_hash(self) -> str:
        return typed_canonical_hash({"sources": [item.to_dict() for item in self.sources]})

    @classmethod
    def build(cls, *, package_slug: str, display_name: str, package_version: str, builder_id: str, sources: tuple[PackageSource, ...], localizations: tuple[LocalizationDecision, ...], metric_contract: MetricContract, claim_contract: PackageClaimContract, spec_payload: Mapping[str, object]) -> "ResearchPackage":
        values = {
            "package_slug": package_slug,
            "display_name": display_name,
            "package_version": package_version,
            "builder_id": builder_id,
            "sources": [item.to_dict() for item in sources],
            "localizations": [item.to_dict() for item in localizations],
            "metric_contract": {"contract_hash": metric_contract.contract_hash, **metric_contract.payload()},
            "claim_contract": {"contract_hash": claim_contract.contract_hash, **claim_contract.payload()},
            "spec_payload": dict(spec_payload),
            "contract_version": RESEARCH_PACKAGE_VERSION,
        }
        package_hash = typed_canonical_hash(values)
        return cls(f"research_package_{package_hash[:16]}", package_hash, package_slug, display_name, package_version, builder_id, sources, localizations, metric_contract, claim_contract, spec_payload)


__all__ = ["LocalizationDecision", "MetricContract", "PACKAGE_CLAIM_LEVELS", "PackageClaimContract", "PackageSource", "RESEARCH_PACKAGE_VERSION", "ResearchPackage", "ResearchPackageError", "SOURCE_PROVENANCE_MODES", "SOURCE_PROVENANCE_VERSION", "SourceProvenance"]


PACKAGE_CLAIM_LEVELS = CLAIM_LEVELS
