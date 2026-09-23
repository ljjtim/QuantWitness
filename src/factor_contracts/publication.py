"""研究准入与发布端共同理解的只读发布身份。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


FACTOR_PUBLICATION_BINDING_VERSION = "factor-publication-binding-v1"


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} 必须是非空字符串")
    return value


def _hash(value: object, field: str) -> str:
    text = _text(value, field)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field} 必须是小写SHA-256")
    return text


def _date(value: object, field: str) -> str:
    text = _text(value, field)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} 必须是ISO日期") from exc


@dataclass(frozen=True)
class FactorImplementationBinding:
    source_database_identity: str
    formula_catalog_hash: str
    code_commit: str
    compute_code_hash: str
    manifest_hash: str

    def __post_init__(self) -> None:
        _hash(self.source_database_identity, "source_database_identity")
        _hash(self.formula_catalog_hash, "formula_catalog_hash")
        _text(self.code_commit, "code_commit")
        _hash(self.compute_code_hash, "compute_code_hash")
        _hash(self.manifest_hash, "manifest_hash")

    def to_dict(self) -> dict[str, str]:
        return {
            "source_database_identity": self.source_database_identity,
            "formula_catalog_hash": self.formula_catalog_hash,
            "code_commit": self.code_commit,
            "compute_code_hash": self.compute_code_hash,
            "manifest_hash": self.manifest_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorImplementationBinding":
        expected = {
            "source_database_identity",
            "formula_catalog_hash",
            "code_commit",
            "compute_code_hash",
            "manifest_hash",
        }
        if set(payload) != expected:
            raise ValueError("FactorImplementationBinding schema 不匹配")
        return cls(**{key: str(payload[key]) for key in expected})


@dataclass(frozen=True)
class FactorStorageBinding:
    storage_table: str
    content_hash: str
    date_start: str
    date_end: str

    def __post_init__(self) -> None:
        _text(self.storage_table, "storage_table")
        _hash(self.content_hash, "content_hash")
        start = _date(self.date_start, "date_start")
        end = _date(self.date_end, "date_end")
        if start > end:
            raise ValueError("storage date_start 不能晚于 date_end")

    def to_dict(self) -> dict[str, str]:
        return {
            "storage_table": self.storage_table,
            "content_hash": self.content_hash,
            "date_start": self.date_start,
            "date_end": self.date_end,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorStorageBinding":
        expected = {"storage_table", "content_hash", "date_start", "date_end"}
        if set(payload) != expected:
            raise ValueError("FactorStorageBinding schema 不匹配")
        return cls(**{key: str(payload[key]) for key in expected})


@dataclass(frozen=True)
class FactorEvidenceBinding:
    recipe_count: int
    enabled_recipe_count: int
    dependency_count: int
    recipe_step_count: int
    source_reference_count: int
    quality_record_count: int
    quality_status_counts: tuple[tuple[str, int], ...]
    numeric_anomaly_count: int
    numeric_anomaly_reason_counts: tuple[tuple[str, int], ...]
    verified_relations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        integer_fields = (
            self.recipe_count,
            self.enabled_recipe_count,
            self.dependency_count,
            self.recipe_step_count,
            self.source_reference_count,
            self.quality_record_count,
            self.numeric_anomaly_count,
        )
        if any(type(value) is not int or value < 0 for value in integer_fields):
            raise ValueError("因子证据计数必须是非负整数")
        if self.enabled_recipe_count > self.recipe_count:
            raise ValueError("启用配方数量不能超过配方总数")
        if self.quality_record_count != self.enabled_recipe_count:
            raise ValueError("质量记录必须逐项覆盖启用配方")
        for field, values in (
            ("quality_status_counts", self.quality_status_counts),
            ("numeric_anomaly_reason_counts", self.numeric_anomaly_reason_counts),
        ):
            if values != tuple(sorted(values)):
                raise ValueError(f"{field} 必须稳定排序")
            if any(not name or type(count) is not int or count <= 0 for name, count in values):
                raise ValueError(f"{field} 条目无效")
        if sum(count for _, count in self.quality_status_counts) != self.quality_record_count:
            raise ValueError("质量状态汇总与记录数不一致")
        if (
            sum(count for _, count in self.numeric_anomaly_reason_counts)
            != self.numeric_anomaly_count
        ):
            raise ValueError("数值异常汇总与记录数不一致")
        if self.verified_relations != tuple(sorted(set(self.verified_relations))):
            raise ValueError("verified_relations 必须唯一并稳定排序")

    def to_dict(self) -> dict[str, object]:
        return {
            "recipe_count": self.recipe_count,
            "enabled_recipe_count": self.enabled_recipe_count,
            "dependency_count": self.dependency_count,
            "recipe_step_count": self.recipe_step_count,
            "source_reference_count": self.source_reference_count,
            "quality_record_count": self.quality_record_count,
            "quality_status_counts": dict(self.quality_status_counts),
            "numeric_anomaly_count": self.numeric_anomaly_count,
            "numeric_anomaly_reason_counts": dict(
                self.numeric_anomaly_reason_counts
            ),
            "verified_relations": list(self.verified_relations),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorEvidenceBinding":
        expected = {
            "recipe_count",
            "enabled_recipe_count",
            "dependency_count",
            "recipe_step_count",
            "source_reference_count",
            "quality_record_count",
            "quality_status_counts",
            "numeric_anomaly_count",
            "numeric_anomaly_reason_counts",
            "verified_relations",
        }
        if set(payload) != expected:
            raise ValueError("FactorEvidenceBinding schema 不匹配")
        quality = payload["quality_status_counts"]
        anomalies = payload["numeric_anomaly_reason_counts"]
        relations = payload["verified_relations"]
        if (
            not isinstance(quality, Mapping)
            or not isinstance(anomalies, Mapping)
            or not isinstance(relations, (list, tuple))
        ):
            raise ValueError("FactorEvidenceBinding 状态汇总无效")
        return cls(
            recipe_count=int(payload["recipe_count"]),
            enabled_recipe_count=int(payload["enabled_recipe_count"]),
            dependency_count=int(payload["dependency_count"]),
            recipe_step_count=int(payload["recipe_step_count"]),
            source_reference_count=int(payload["source_reference_count"]),
            quality_record_count=int(payload["quality_record_count"]),
            quality_status_counts=tuple(
                sorted((str(key), int(value)) for key, value in quality.items())
            ),
            numeric_anomaly_count=int(payload["numeric_anomaly_count"]),
            numeric_anomaly_reason_counts=tuple(
                sorted((str(key), int(value)) for key, value in anomalies.items())
            ),
            verified_relations=tuple(str(item) for item in relations),
        )


@dataclass(frozen=True)
class FactorPublicationBinding:
    publication_id: str
    compute_run_id: str
    catalog_hash: str
    published_at: str
    date_start: str
    date_end: str
    factor_count: int
    row_count: int
    schema_version: str
    availability_policy_ref: str
    revision_policy_ref: str
    implementation: FactorImplementationBinding
    storage: tuple[FactorStorageBinding, ...]
    evidence: FactorEvidenceBinding
    contract_version: str = FACTOR_PUBLICATION_BINDING_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != FACTOR_PUBLICATION_BINDING_VERSION:
            raise ValueError("FactorPublicationBinding contract_version 不受支持")
        _text(self.publication_id, "publication_id")
        _text(self.compute_run_id, "compute_run_id")
        _hash(self.catalog_hash, "catalog_hash")
        _text(self.published_at, "published_at")
        start = _date(self.date_start, "date_start")
        end = _date(self.date_end, "date_end")
        if start > end:
            raise ValueError("publication date_start 不能晚于 date_end")
        if self.factor_count <= 0 or self.row_count <= 0:
            raise ValueError("publication 数量必须为正整数")
        _text(self.schema_version, "schema_version")
        _text(self.availability_policy_ref, "availability_policy_ref")
        _text(self.revision_policy_ref, "revision_policy_ref")
        if self.implementation.formula_catalog_hash != self.catalog_hash:
            raise ValueError("计算运行与publication catalog_hash不一致")
        if self.evidence.enabled_recipe_count != self.factor_count:
            raise ValueError("publication factor_count与启用配方数量不一致")
        if not self.storage:
            raise ValueError("FactorPublicationBinding 至少绑定一张存储表")
        names = [item.storage_table for item in self.storage]
        if names != sorted(set(names)):
            raise ValueError("storage 必须按表名排序且不能重复")

    @property
    def storage_map(self) -> dict[str, FactorStorageBinding]:
        return {item.storage_table: item for item in self.storage}

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "publication_id": self.publication_id,
            "compute_run_id": self.compute_run_id,
            "catalog_hash": self.catalog_hash,
            "published_at": self.published_at,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "factor_count": self.factor_count,
            "row_count": self.row_count,
            "schema_version": self.schema_version,
            "availability_policy_ref": self.availability_policy_ref,
            "revision_policy_ref": self.revision_policy_ref,
            "implementation": self.implementation.to_dict(),
            "storage": [item.to_dict() for item in self.storage],
            "evidence": self.evidence.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorPublicationBinding":
        expected = {
            "contract_version", "publication_id", "compute_run_id", "catalog_hash",
            "published_at", "date_start", "date_end", "factor_count", "row_count",
            "schema_version", "availability_policy_ref", "revision_policy_ref",
            "implementation", "storage",
            "evidence",
        }
        if set(payload) != expected:
            raise ValueError("FactorPublicationBinding schema 不匹配")
        implementation = payload["implementation"]
        storage = payload["storage"]
        evidence = payload["evidence"]
        if (
            not isinstance(implementation, Mapping)
            or not isinstance(storage, list)
            or not isinstance(evidence, Mapping)
        ):
            raise ValueError("FactorPublicationBinding 嵌套结构无效")
        return cls(
            publication_id=str(payload["publication_id"]),
            compute_run_id=str(payload["compute_run_id"]),
            catalog_hash=str(payload["catalog_hash"]),
            published_at=str(payload["published_at"]),
            date_start=str(payload["date_start"]),
            date_end=str(payload["date_end"]),
            factor_count=int(payload["factor_count"]),
            row_count=int(payload["row_count"]),
            schema_version=str(payload["schema_version"]),
            availability_policy_ref=str(payload["availability_policy_ref"]),
            revision_policy_ref=str(payload["revision_policy_ref"]),
            implementation=FactorImplementationBinding.from_dict(implementation),
            storage=tuple(FactorStorageBinding.from_dict(item) for item in storage),
            evidence=FactorEvidenceBinding.from_dict(evidence),
            contract_version=str(payload["contract_version"]),
        )
