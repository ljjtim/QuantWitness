"""通用 Feature、Label、Estimand 与 Hypothesis 研究语义合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Mapping, Sequence

from research_pipeline.platform import MainlineError, typed_canonical_hash
from research_pipeline.platform.claim_levels import CLAIM_LEVELS, weakest_claim_level


RESEARCH_TIME_WINDOW_VERSION = "research-time-window-v1"
FEATURE_SET_ARTIFACT_VERSION = "research-feature-set-artifact-v2"
LABEL_ARTIFACT_VERSION = "research-label-artifact-v2"
ESTIMAND_SPEC_VERSION = "research-estimand-spec-v1"
HYPOTHESIS_SPEC_VERSION = "research-hypothesis-spec-v1"
RESEARCH_SEMANTICS_VERSION = "research-semantics-v1"

_ID = re.compile(r"^[a-z][a-z0-9_.:-]{1,127}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
class ResearchSemanticsError(MainlineError):
    """研究语义不闭合、时间倒置或身份漂移。"""

    error_code = "research_semantics_invalid"


def _require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ResearchSemanticsError(f"{field} 必须是稳定 ID")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise ResearchSemanticsError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_aware(value: object, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ResearchSemanticsError(f"{field} 必须是带时区时间")
    return value


def _parse_aware(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ResearchSemanticsError(f"{field} 必须是 ISO 时间")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ResearchSemanticsError(f"{field} 必须是 ISO 时间") from exc
    return _require_aware(parsed, field)


def _ordered_ids(values: Sequence[object], field: str) -> tuple[str, ...]:
    normalized = tuple(_require_id(item, f"{field}[]") for item in values)
    if not normalized or normalized != tuple(sorted(set(normalized))):
        raise ResearchSemanticsError(f"{field} 必须非空、唯一并规范排序")
    return normalized


def _ordered_text(values: Sequence[object], field: str) -> tuple[str, ...]:
    normalized = tuple(
        item.strip() if isinstance(item, str) else ""
        for item in values
    )
    if (
        not normalized
        or any(not item for item in normalized)
        or len(normalized) != len(set(normalized))
    ):
        raise ResearchSemanticsError(f"{field} 必须是非空唯一字符串序列")
    return normalized


def _exact(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(payload) != expected:
        raise ResearchSemanticsError(
            f"{field} schema 不匹配；缺失={sorted(expected - set(payload))}，"
            f"未知={sorted(set(payload) - expected)}"
        )


@dataclass(frozen=True)
class ResearchTimeWindow:
    observation_at: datetime
    available_at: datetime
    window_start: datetime
    window_end: datetime
    contract_version: str = RESEARCH_TIME_WINDOW_VERSION

    def __post_init__(self) -> None:
        for field in ("observation_at", "available_at", "window_start", "window_end"):
            _require_aware(getattr(self, field), field)
        if self.window_start > self.window_end:
            raise ResearchSemanticsError("研究窗口必须满足 window_start <= window_end")
        if self.available_at < self.observation_at:
            raise ResearchSemanticsError("available_at 不能早于 observation_at")
        if self.contract_version != RESEARCH_TIME_WINDOW_VERSION:
            raise ResearchSemanticsError("ResearchTimeWindow 版本不受支持")

    def to_dict(self) -> dict[str, str]:
        return {
            "observation_at": self.observation_at.isoformat(),
            "available_at": self.available_at.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResearchTimeWindow":
        expected = {
            "observation_at", "available_at", "window_start", "window_end",
            "contract_version",
        }
        _exact(payload, expected, "ResearchTimeWindow")
        return cls(
            observation_at=_parse_aware(payload["observation_at"], "observation_at"),
            available_at=_parse_aware(payload["available_at"], "available_at"),
            window_start=_parse_aware(payload["window_start"], "window_start"),
            window_end=_parse_aware(payload["window_end"], "window_end"),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class FeatureSetArtifact:
    feature_id: str
    artifact_id: str
    entity_keys: tuple[str, ...]
    value_fields: tuple[str, ...]
    time_window: ResearchTimeWindow
    schema_hash: str
    transform_lineage_hash: str
    source_revision_hash: str
    preprocessing_order: tuple[str, ...]
    artifact_hash: str
    contract_version: str = FEATURE_SET_ARTIFACT_VERSION

    def __post_init__(self) -> None:
        _require_id(self.feature_id, "feature_id")
        _require_id(self.artifact_id, "feature artifact_id")
        object.__setattr__(self, "entity_keys", _ordered_ids(self.entity_keys, "feature entity_keys"))
        object.__setattr__(self, "value_fields", _ordered_ids(self.value_fields, "feature value_fields"))
        for field in ("schema_hash", "transform_lineage_hash", "source_revision_hash"):
            _require_hash(getattr(self, field), f"feature {field}")
        object.__setattr__(
            self,
            "preprocessing_order",
            _ordered_text(self.preprocessing_order, "feature preprocessing_order"),
        )
        if self.time_window.window_end > self.time_window.observation_at:
            raise ResearchSemanticsError("Feature 窗口不得越过 observation_at")
        if self.contract_version != FEATURE_SET_ARTIFACT_VERSION:
            raise ResearchSemanticsError("FeatureSetArtifact 版本不受支持")
        if self.artifact_hash != typed_canonical_hash(self.payload()):
            raise ResearchSemanticsError("FeatureSetArtifact hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "feature_id": self.feature_id,
            "artifact_id": self.artifact_id,
            "entity_keys": list(self.entity_keys),
            "value_fields": list(self.value_fields),
            "time_window": self.time_window.to_dict(),
            "schema_hash": self.schema_hash,
            "transform_lineage_hash": self.transform_lineage_hash,
            "source_revision_hash": self.source_revision_hash,
            "preprocessing_order": list(self.preprocessing_order),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "artifact_hash": self.artifact_hash}

    @classmethod
    def build(cls, **kwargs: object) -> "FeatureSetArtifact":
        values = {
            **kwargs,
            "contract_version": FEATURE_SET_ARTIFACT_VERSION,
        }
        payload = {
            "feature_id": values["feature_id"],
            "artifact_id": values["artifact_id"],
            "entity_keys": list(values["entity_keys"]),
            "value_fields": list(values["value_fields"]),
            "time_window": values["time_window"].to_dict(),
            "schema_hash": values["schema_hash"],
            "transform_lineage_hash": values["transform_lineage_hash"],
            "source_revision_hash": values["source_revision_hash"],
            "preprocessing_order": list(values["preprocessing_order"]),
            "contract_version": FEATURE_SET_ARTIFACT_VERSION,
        }
        return cls(**values, artifact_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "FeatureSetArtifact":
        expected = {
            "feature_id", "artifact_id", "entity_keys", "value_fields", "time_window",
            "schema_hash", "transform_lineage_hash", "source_revision_hash",
            "preprocessing_order", "artifact_hash", "contract_version",
        }
        _exact(payload, expected, "FeatureSetArtifact")
        if not isinstance(payload["time_window"], Mapping):
            raise ResearchSemanticsError("FeatureSetArtifact time_window 必须是映射")
        for field in ("entity_keys", "value_fields", "preprocessing_order"):
            if not isinstance(payload[field], (list, tuple)):
                raise ResearchSemanticsError(f"FeatureSetArtifact {field} 必须是序列")
        return cls(
            feature_id=str(payload["feature_id"]),
            artifact_id=str(payload["artifact_id"]),
            entity_keys=tuple(payload["entity_keys"]),
            value_fields=tuple(payload["value_fields"]),
            time_window=ResearchTimeWindow.from_dict(payload["time_window"]),
            schema_hash=str(payload["schema_hash"]),
            transform_lineage_hash=str(payload["transform_lineage_hash"]),
            source_revision_hash=str(payload["source_revision_hash"]),
            preprocessing_order=tuple(payload["preprocessing_order"]),
            artifact_hash=str(payload["artifact_hash"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class LabelArtifact:
    label_id: str
    artifact_id: str
    entity_keys: tuple[str, ...]
    value_field: str
    time_window: ResearchTimeWindow
    schema_hash: str
    source_lineage_hash: str
    source_revision_hash: str
    visibility_policy_hash: str
    revision_policy: str
    training_only: bool
    artifact_hash: str
    contract_version: str = LABEL_ARTIFACT_VERSION

    def __post_init__(self) -> None:
        _require_id(self.label_id, "label_id")
        _require_id(self.artifact_id, "label artifact_id")
        _require_id(self.value_field, "label value_field")
        object.__setattr__(self, "entity_keys", _ordered_ids(self.entity_keys, "label entity_keys"))
        for field in (
            "schema_hash", "source_lineage_hash", "source_revision_hash",
            "visibility_policy_hash",
        ):
            _require_hash(getattr(self, field), f"label {field}")
        if self.revision_policy not in {
            "as_published",
            "point_in_time",
            "current_snapshot",
        }:
            raise ResearchSemanticsError(
                "Label revision_policy 只允许 as_published/point_in_time/current_snapshot"
            )
        if type(self.training_only) is not bool:
            raise ResearchSemanticsError("Label training_only 必须是 bool")
        if not (
            self.time_window.observation_at <= self.time_window.window_start
            <= self.time_window.window_end <= self.time_window.available_at
        ):
            raise ResearchSemanticsError(
                "Label 必须满足 observation_at <= window_start <= window_end <= available_at；"
                "实际首观测仍须由逐行时间合同证明严格晚于决策"
            )
        if self.contract_version != LABEL_ARTIFACT_VERSION:
            raise ResearchSemanticsError("LabelArtifact 版本不受支持")
        if self.artifact_hash != typed_canonical_hash(self.payload()):
            raise ResearchSemanticsError("LabelArtifact hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "label_id": self.label_id,
            "artifact_id": self.artifact_id,
            "entity_keys": list(self.entity_keys),
            "value_field": self.value_field,
            "time_window": self.time_window.to_dict(),
            "schema_hash": self.schema_hash,
            "source_lineage_hash": self.source_lineage_hash,
            "source_revision_hash": self.source_revision_hash,
            "visibility_policy_hash": self.visibility_policy_hash,
            "revision_policy": self.revision_policy,
            "training_only": self.training_only,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "artifact_hash": self.artifact_hash}

    @classmethod
    def build(cls, **kwargs: object) -> "LabelArtifact":
        values = {**kwargs, "contract_version": LABEL_ARTIFACT_VERSION}
        time_window = values["time_window"]
        payload = {
            "label_id": values["label_id"],
            "artifact_id": values["artifact_id"],
            "entity_keys": list(values["entity_keys"]),
            "value_field": values["value_field"],
            "time_window": time_window.to_dict(),
            "schema_hash": values["schema_hash"],
            "source_lineage_hash": values["source_lineage_hash"],
            "source_revision_hash": values["source_revision_hash"],
            "visibility_policy_hash": values["visibility_policy_hash"],
            "revision_policy": values["revision_policy"],
            "training_only": values["training_only"],
            "contract_version": LABEL_ARTIFACT_VERSION,
        }
        return cls(**values, artifact_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "LabelArtifact":
        expected = {
            "label_id", "artifact_id", "entity_keys", "value_field", "time_window",
            "schema_hash", "source_lineage_hash", "source_revision_hash",
            "visibility_policy_hash", "revision_policy", "training_only", "artifact_hash",
            "contract_version",
        }
        _exact(payload, expected, "LabelArtifact")
        if not isinstance(payload["entity_keys"], (list, tuple)) or not isinstance(
            payload["time_window"], Mapping
        ):
            raise ResearchSemanticsError("LabelArtifact 序列或 time_window 类型无效")
        return cls(
            label_id=str(payload["label_id"]),
            artifact_id=str(payload["artifact_id"]),
            entity_keys=tuple(payload["entity_keys"]),
            value_field=str(payload["value_field"]),
            time_window=ResearchTimeWindow.from_dict(payload["time_window"]),
            schema_hash=str(payload["schema_hash"]),
            source_lineage_hash=str(payload["source_lineage_hash"]),
            source_revision_hash=str(payload["source_revision_hash"]),
            visibility_policy_hash=str(payload["visibility_policy_hash"]),
            revision_policy=str(payload["revision_policy"]),
            training_only=payload["training_only"],
            artifact_hash=str(payload["artifact_hash"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class EstimandSpec:
    estimand_id: str
    label_id: str
    population: str
    sample_policy: str
    statistic: str
    unit: str
    direction: str
    metric_refs: tuple[str, ...]
    estimand_hash: str
    contract_version: str = ESTIMAND_SPEC_VERSION

    def __post_init__(self) -> None:
        for field in ("estimand_id", "label_id", "population", "sample_policy", "statistic", "unit"):
            _require_id(getattr(self, field), f"estimand {field}")
        if self.direction not in {"positive", "negative", "two_sided", "descriptive"}:
            raise ResearchSemanticsError("Estimand direction 不受支持")
        refs = tuple(self.metric_refs)
        if not refs or refs != tuple(sorted(set(refs))) or any("@" not in item for item in refs):
            raise ResearchSemanticsError("Estimand metric_refs 必须非空、唯一并规范排序")
        if self.contract_version != ESTIMAND_SPEC_VERSION:
            raise ResearchSemanticsError("EstimandSpec 版本不受支持")
        if self.estimand_hash != typed_canonical_hash(self.payload()):
            raise ResearchSemanticsError("EstimandSpec hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "estimand_id": self.estimand_id,
            "label_id": self.label_id,
            "population": self.population,
            "sample_policy": self.sample_policy,
            "statistic": self.statistic,
            "unit": self.unit,
            "direction": self.direction,
            "metric_refs": list(self.metric_refs),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "estimand_hash": self.estimand_hash}

    @classmethod
    def build(cls, **kwargs: object) -> "EstimandSpec":
        refs = tuple(sorted(kwargs["metric_refs"]))
        values = {**kwargs, "metric_refs": refs, "contract_version": ESTIMAND_SPEC_VERSION}
        payload = {
            key: value
            for key, value in values.items()
            if key != "metric_refs"
        }
        payload["metric_refs"] = list(refs)
        return cls(**values, estimand_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "EstimandSpec":
        expected = {
            "estimand_id", "label_id", "population", "sample_policy", "statistic",
            "unit", "direction", "metric_refs", "estimand_hash", "contract_version",
        }
        _exact(payload, expected, "EstimandSpec")
        if not isinstance(payload["metric_refs"], (list, tuple)):
            raise ResearchSemanticsError("Estimand metric_refs 必须是序列")
        return cls(
            estimand_id=str(payload["estimand_id"]),
            label_id=str(payload["label_id"]),
            population=str(payload["population"]),
            sample_policy=str(payload["sample_policy"]),
            statistic=str(payload["statistic"]),
            unit=str(payload["unit"]),
            direction=str(payload["direction"]),
            metric_refs=tuple(payload["metric_refs"]),
            estimand_hash=str(payload["estimand_hash"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class HypothesisSpec:
    hypothesis_id: str
    estimand_id: str
    hypothesis_kind: str
    direction: str
    primary_metric_ref: str | None
    preregistration_hash: str | None
    claim_ceiling: str
    hypothesis_hash: str
    contract_version: str = HYPOTHESIS_SPEC_VERSION

    def __post_init__(self) -> None:
        _require_id(self.hypothesis_id, "hypothesis_id")
        _require_id(self.estimand_id, "hypothesis estimand_id")
        if self.hypothesis_kind not in {"primary", "exploratory"}:
            raise ResearchSemanticsError("hypothesis_kind 不受支持")
        if self.direction not in {"positive", "negative", "two_sided"}:
            raise ResearchSemanticsError("Hypothesis direction 不受支持")
        if self.claim_ceiling not in CLAIM_LEVELS:
            raise ResearchSemanticsError("Hypothesis claim_ceiling 不受支持")
        if self.hypothesis_kind == "primary":
            if not isinstance(self.primary_metric_ref, str) or "@" not in self.primary_metric_ref:
                raise ResearchSemanticsError("primary hypothesis 必须声明 primary metric")
            _require_hash(self.preregistration_hash, "hypothesis preregistration_hash")
        elif self.claim_ceiling != "research_observation":
            raise ResearchSemanticsError("exploratory hypothesis 必须降级到 research_observation")
        if self.contract_version != HYPOTHESIS_SPEC_VERSION:
            raise ResearchSemanticsError("HypothesisSpec 版本不受支持")
        if self.hypothesis_hash != typed_canonical_hash(self.payload()):
            raise ResearchSemanticsError("HypothesisSpec hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "estimand_id": self.estimand_id,
            "hypothesis_kind": self.hypothesis_kind,
            "direction": self.direction,
            "primary_metric_ref": self.primary_metric_ref,
            "preregistration_hash": self.preregistration_hash,
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "hypothesis_hash": self.hypothesis_hash}

    @classmethod
    def build(
        cls,
        *,
        hypothesis_id: str,
        estimand_id: str,
        hypothesis_kind: str,
        direction: str,
        primary_metric_ref: str | None = None,
        preregistration_hash: str | None = None,
        requested_claim_level: str = "research_observation",
    ) -> "HypothesisSpec":
        ceiling = (
            requested_claim_level
            if hypothesis_kind == "primary" and preregistration_hash is not None
            else "research_observation"
        )
        payload = {
            "hypothesis_id": hypothesis_id,
            "estimand_id": estimand_id,
            "hypothesis_kind": hypothesis_kind,
            "direction": direction,
            "primary_metric_ref": primary_metric_ref,
            "preregistration_hash": preregistration_hash,
            "claim_ceiling": ceiling,
            "contract_version": HYPOTHESIS_SPEC_VERSION,
        }
        return cls(**payload, hypothesis_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "HypothesisSpec":
        expected = {
            "hypothesis_id", "estimand_id", "hypothesis_kind", "direction",
            "primary_metric_ref", "preregistration_hash", "claim_ceiling",
            "hypothesis_hash", "contract_version",
        }
        _exact(payload, expected, "HypothesisSpec")
        return cls(
            hypothesis_id=str(payload["hypothesis_id"]),
            estimand_id=str(payload["estimand_id"]),
            hypothesis_kind=str(payload["hypothesis_kind"]),
            direction=str(payload["direction"]),
            primary_metric_ref=(
                None if payload["primary_metric_ref"] is None
                else str(payload["primary_metric_ref"])
            ),
            preregistration_hash=(
                None if payload["preregistration_hash"] is None
                else str(payload["preregistration_hash"])
            ),
            claim_ceiling=str(payload["claim_ceiling"]),
            hypothesis_hash=str(payload["hypothesis_hash"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class ResearchSemantics:
    decision_at: datetime
    features: tuple[FeatureSetArtifact, ...]
    labels: tuple[LabelArtifact, ...]
    estimands: tuple[EstimandSpec, ...]
    hypotheses: tuple[HypothesisSpec, ...]
    semantics_hash: str
    contract_version: str = RESEARCH_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        _require_aware(self.decision_at, "research decision_at")
        for field, values, identity in (
            ("features", self.features, "feature_id"),
            ("labels", self.labels, "label_id"),
            ("estimands", self.estimands, "estimand_id"),
            ("hypotheses", self.hypotheses, "hypothesis_id"),
        ):
            ids = tuple(getattr(item, identity) for item in values)
            if not ids or ids != tuple(sorted(set(ids))):
                raise ResearchSemanticsError(f"research {field} 必须非空、唯一并规范排序")
        if any(item.time_window.available_at > self.decision_at for item in self.features):
            raise ResearchSemanticsError("Feature 在 decision_at 后才可见，存在前视偏差")
        if any(item.time_window.observation_at > self.decision_at for item in self.labels):
            raise ResearchSemanticsError("Label observation_at 不能晚于 decision_at")
        feature_keys = {item.entity_keys for item in self.features}
        label_keys = {item.entity_keys for item in self.labels}
        if len(feature_keys | label_keys) != 1:
            raise ResearchSemanticsError("Feature/Label entity key 不一致")
        label_ids = {item.label_id for item in self.labels}
        if any(item.label_id not in label_ids for item in self.estimands):
            raise ResearchSemanticsError("Estimand 引用了未知 Label")
        estimands = {item.estimand_id: item for item in self.estimands}
        for hypothesis in self.hypotheses:
            estimand = estimands.get(hypothesis.estimand_id)
            if estimand is None:
                raise ResearchSemanticsError("Hypothesis 引用了未知 Estimand")
            if (
                hypothesis.primary_metric_ref is not None
                and hypothesis.primary_metric_ref not in estimand.metric_refs
            ):
                raise ResearchSemanticsError("Hypothesis primary metric 不属于对应 Estimand")
            if hypothesis.direction != estimand.direction and estimand.direction != "descriptive":
                raise ResearchSemanticsError("Hypothesis 与 Estimand direction 不一致")
        if self.contract_version != RESEARCH_SEMANTICS_VERSION:
            raise ResearchSemanticsError("ResearchSemantics 版本不受支持")
        if self.semantics_hash != typed_canonical_hash(self.payload()):
            raise ResearchSemanticsError("ResearchSemantics hash 不一致")

    @property
    def claim_ceiling(self) -> str:
        primary = tuple(
            item for item in self.hypotheses if item.hypothesis_kind == "primary"
        )
        if not primary:
            return "research_observation"
        return weakest_claim_level(*(item.claim_ceiling for item in primary))

    @property
    def metric_refs(self) -> tuple[str, ...]:
        return tuple(sorted({ref for item in self.estimands for ref in item.metric_refs}))

    def payload(self) -> dict[str, object]:
        return {
            "decision_at": self.decision_at.isoformat(),
            "features": [item.to_dict() for item in self.features],
            "labels": [item.to_dict() for item in self.labels],
            "estimands": [item.to_dict() for item in self.estimands],
            "hypotheses": [item.to_dict() for item in self.hypotheses],
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "semantics_hash": self.semantics_hash}

    @classmethod
    def build(
        cls,
        *,
        decision_at: datetime,
        features: Sequence[FeatureSetArtifact],
        labels: Sequence[LabelArtifact],
        estimands: Sequence[EstimandSpec],
        hypotheses: Sequence[HypothesisSpec],
    ) -> "ResearchSemantics":
        ordered_features = tuple(sorted(features, key=lambda item: item.feature_id))
        ordered_labels = tuple(sorted(labels, key=lambda item: item.label_id))
        ordered_estimands = tuple(sorted(estimands, key=lambda item: item.estimand_id))
        ordered_hypotheses = tuple(
            sorted(hypotheses, key=lambda item: item.hypothesis_id)
        )
        primary = tuple(
            item for item in ordered_hypotheses
            if item.hypothesis_kind == "primary"
        )
        claim_ceiling = (
            "research_observation"
            if not primary
            else weakest_claim_level(*(item.claim_ceiling for item in primary))
        )
        values = {
            "decision_at": decision_at,
            "features": ordered_features,
            "labels": ordered_labels,
            "estimands": ordered_estimands,
            "hypotheses": ordered_hypotheses,
            "contract_version": RESEARCH_SEMANTICS_VERSION,
        }
        payload = {
            "decision_at": decision_at.isoformat(),
            "features": [item.to_dict() for item in ordered_features],
            "labels": [item.to_dict() for item in ordered_labels],
            "estimands": [item.to_dict() for item in ordered_estimands],
            "hypotheses": [item.to_dict() for item in ordered_hypotheses],
            "claim_ceiling": claim_ceiling,
            "contract_version": RESEARCH_SEMANTICS_VERSION,
        }
        return cls(**values, semantics_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResearchSemantics":
        expected = {
            "decision_at", "features", "labels", "estimands", "hypotheses",
            "claim_ceiling", "semantics_hash", "contract_version",
        }
        _exact(payload, expected, "ResearchSemantics")
        for field in ("features", "labels", "estimands", "hypotheses"):
            if not isinstance(payload[field], (list, tuple)) or any(
                not isinstance(item, Mapping) for item in payload[field]
            ):
                raise ResearchSemanticsError(f"ResearchSemantics {field} 必须是映射序列")
        result = cls(
            decision_at=_parse_aware(payload["decision_at"], "decision_at"),
            features=tuple(FeatureSetArtifact.from_dict(item) for item in payload["features"]),
            labels=tuple(LabelArtifact.from_dict(item) for item in payload["labels"]),
            estimands=tuple(EstimandSpec.from_dict(item) for item in payload["estimands"]),
            hypotheses=tuple(HypothesisSpec.from_dict(item) for item in payload["hypotheses"]),
            semantics_hash=str(payload["semantics_hash"]),
            contract_version=str(payload["contract_version"]),
        )
        if payload["claim_ceiling"] != result.claim_ceiling:
            raise ResearchSemanticsError("ResearchSemantics claim_ceiling 漂移")
        return result


__all__ = [
    "ESTIMAND_SPEC_VERSION",
    "FEATURE_SET_ARTIFACT_VERSION",
    "HYPOTHESIS_SPEC_VERSION",
    "LABEL_ARTIFACT_VERSION",
    "RESEARCH_SEMANTICS_VERSION",
    "RESEARCH_TIME_WINDOW_VERSION",
    "EstimandSpec",
    "FeatureSetArtifact",
    "HypothesisSpec",
    "LabelArtifact",
    "ResearchSemantics",
    "ResearchSemanticsError",
    "ResearchTimeWindow",
]
