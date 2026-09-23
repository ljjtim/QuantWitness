"""受信指标定义、可达性证明与指标工件合同。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import importlib.util
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping, Sequence

from research_pipeline.platform import MainlineError, typed_canonical_hash
from research_pipeline.platform.semantic_governance import (
    MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    validate_public_semantic_inventory,
)


METRIC_DEFINITION_VERSION = "research-metric-definition-v2"
METRIC_ARTIFACT_VERSION = "research-metric-artifact-v1"
METRIC_REACHABILITY_VERSION = "research-metric-reachability-v2"
BUILTIN_METRIC_IDENTITIES = frozenset({
    "data.row_count@1.0.0",
    "minute.row_count@1.0.0",
    "statistics.adjusted_p@1.0.0",
})
_ID = re.compile(r"^[a-z][a-z0-9_.-]{1,127}$")
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_HASH = re.compile(r"^[0-9a-f]{64}$")


class MetricContractError(MainlineError):
    error_code = "metric_contract_invalid"


class UnknownMetricError(MetricContractError):
    error_code = "metric_unknown"


class UnreachableMetricError(MetricContractError):
    error_code = "metric_unreachable"


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MetricContractError(f"{field} 必须是非空字符串")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise MetricContractError(f"{field} 必须是 sha256 摘要")
    return value


def _string_mapping(value: Mapping[str, str], field: str) -> Mapping[str, str]:
    normalized = {
        _text(key, f"{field}.key"): _text(item, f"{field}.{key}")
        for key, item in value.items()
    }
    if not normalized or len(normalized) != len(value):
        raise MetricContractError(f"{field} 必须是非空字符串映射")
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    version: str
    input_artifact_type: str
    result_schema_id: str
    output_schema: Mapping[str, str]
    unit: str
    frequency: str
    annualization_policy: str
    risk_free_rate_policy: str
    null_policy: str
    direction: str
    implementation_ref: str
    implementation_digest: str
    definition_digest: str
    contract_version: str = METRIC_DEFINITION_VERSION

    def __post_init__(self) -> None:
        if not _ID.fullmatch(self.metric_id) or not _VERSION.fullmatch(self.version):
            raise MetricContractError("MetricDefinition id/version 无效")
        if not _ID.fullmatch(self.input_artifact_type) or not _ID.fullmatch(
            self.result_schema_id
        ):
            raise MetricContractError("MetricDefinition artifact/result schema 无效")
        object.__setattr__(
            self,
            "output_schema",
            _string_mapping(self.output_schema, "output_schema"),
        )
        for field in (
            "unit",
            "frequency",
            "annualization_policy",
            "risk_free_rate_policy",
            "null_policy",
            "implementation_ref",
        ):
            _text(getattr(self, field), field)
        if self.direction not in {"higher_is_better", "lower_is_better", "neutral"}:
            raise MetricContractError("MetricDefinition direction 无效")
        _digest(self.implementation_digest, "implementation_digest")
        if self.contract_version != METRIC_DEFINITION_VERSION:
            raise MetricContractError("MetricDefinition contract version 不受支持")
        if self.definition_digest != typed_canonical_hash(self.payload()):
            raise MetricContractError("MetricDefinition digest 不一致")

    @property
    def metric_ref(self) -> str:
        return f"{self.metric_id}@{self.version}"

    def payload(self) -> dict[str, object]:
        return {
            "metric_id": self.metric_id,
            "version": self.version,
            "input_artifact_type": self.input_artifact_type,
            "result_schema_id": self.result_schema_id,
            "output_schema": dict(self.output_schema),
            "unit": self.unit,
            "frequency": self.frequency,
            "annualization_policy": self.annualization_policy,
            "risk_free_rate_policy": self.risk_free_rate_policy,
            "null_policy": self.null_policy,
            "direction": self.direction,
            "implementation_ref": self.implementation_ref,
            "implementation_digest": self.implementation_digest,
            "contract_version": self.contract_version,
        }

    @classmethod
    def build(
        cls,
        *,
        metric_id: str,
        version: str,
        input_artifact_type: str,
        result_schema_id: str,
        output_schema: Mapping[str, str],
        unit: str,
        frequency: str,
        annualization_policy: str,
        risk_free_rate_policy: str,
        null_policy: str,
        direction: str,
        implementation_ref: str,
        implementation_digest: str,
    ) -> "MetricDefinition":
        values = {
            "metric_id": metric_id,
            "version": version,
            "input_artifact_type": input_artifact_type,
            "result_schema_id": result_schema_id,
            "output_schema": dict(sorted(output_schema.items())),
            "unit": unit,
            "frequency": frequency,
            "annualization_policy": annualization_policy,
            "risk_free_rate_policy": risk_free_rate_policy,
            "null_policy": null_policy,
            "direction": direction,
            "implementation_ref": implementation_ref,
            "implementation_digest": implementation_digest,
            "contract_version": METRIC_DEFINITION_VERSION,
        }
        return cls(**values, definition_digest=typed_canonical_hash(values))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "MetricDefinition":
        expected = {
            "metric_id", "version", "input_artifact_type", "result_schema_id",
            "output_schema", "unit", "frequency", "annualization_policy",
            "risk_free_rate_policy", "null_policy", "direction",
            "implementation_ref", "implementation_digest", "definition_digest",
            "contract_version",
        }
        if set(payload) != expected or not isinstance(payload["output_schema"], Mapping):
            raise MetricContractError("MetricDefinition schema 无效")
        return cls(
            **{key: payload[key] for key in expected if key != "output_schema"},
            output_schema=dict(payload["output_schema"]),
        )


@dataclass(frozen=True)
class MetricReachabilityProof:
    metric_ref: str
    definition_digest: str
    producer_node_id: str
    producer_port: str
    artifact_type: str
    result_table_id: str
    result_schema_id: str
    result_path_prefix: str
    proof_digest: str
    contract_version: str = METRIC_REACHABILITY_VERSION

    def __post_init__(self) -> None:
        _text(self.metric_ref, "metric_ref")
        _digest(self.definition_digest, "definition_digest")
        for field in (
            "producer_node_id",
            "producer_port",
            "artifact_type",
            "result_table_id",
            "result_schema_id",
            "result_path_prefix",
        ):
            _text(getattr(self, field), field)
        if self.contract_version != METRIC_REACHABILITY_VERSION:
            raise MetricContractError("MetricReachabilityProof 版本不受支持")
        if self.proof_digest != typed_canonical_hash(self.payload()):
            raise MetricContractError("MetricReachabilityProof digest 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "metric_ref": self.metric_ref,
            "definition_digest": self.definition_digest,
            "producer_node_id": self.producer_node_id,
            "producer_port": self.producer_port,
            "artifact_type": self.artifact_type,
            "result_table_id": self.result_table_id,
            "result_schema_id": self.result_schema_id,
            "result_path_prefix": self.result_path_prefix,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "proof_digest": self.proof_digest}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "MetricReachabilityProof":
        expected = {
            "metric_ref",
            "definition_digest",
            "producer_node_id",
            "producer_port",
            "artifact_type",
            "result_table_id",
            "result_schema_id",
            "result_path_prefix",
            "proof_digest",
            "contract_version",
        }
        if set(payload) != expected or any(
            not isinstance(payload[field], str) for field in expected
        ):
            raise MetricContractError("MetricReachabilityProof schema 无效")
        return cls(
            metric_ref=str(payload["metric_ref"]),
            definition_digest=str(payload["definition_digest"]),
            producer_node_id=str(payload["producer_node_id"]),
            producer_port=str(payload["producer_port"]),
            artifact_type=str(payload["artifact_type"]),
            result_table_id=str(payload["result_table_id"]),
            result_schema_id=str(payload["result_schema_id"]),
            result_path_prefix=str(payload["result_path_prefix"]),
            proof_digest=str(payload["proof_digest"]),
            contract_version=str(payload["contract_version"]),
        )

    @classmethod
    def build(
        cls,
        definition: MetricDefinition,
        producer: tuple[str, str, str],
        result_binding: tuple[str, str, str],
    ) -> "MetricReachabilityProof":
        node_id, port, artifact_type = producer
        table_id, schema_id, path_prefix = result_binding
        values = {
            "metric_ref": definition.metric_ref,
            "definition_digest": definition.definition_digest,
            "producer_node_id": node_id,
            "producer_port": port,
            "artifact_type": artifact_type,
            "result_table_id": table_id,
            "result_schema_id": schema_id,
            "result_path_prefix": path_prefix,
            "contract_version": METRIC_REACHABILITY_VERSION,
        }
        return cls(**values, proof_digest=typed_canonical_hash(values))


@dataclass(frozen=True)
class MetricArtifact:
    metric_ref: str
    definition_digest: str
    unit: str
    sample_scope: Mapping[str, str]
    implementation_digest: str
    source_artifact_digests: Mapping[str, str]
    values: tuple[Mapping[str, object], ...]
    artifact_digest: str
    contract_version: str = METRIC_ARTIFACT_VERSION

    def __post_init__(self) -> None:
        _text(self.metric_ref, "metric_ref")
        metric_id, separator, version = self.metric_ref.rpartition("@")
        if (
            separator != "@"
            or not _ID.fullmatch(metric_id)
            or not _VERSION.fullmatch(version)
        ):
            raise MetricContractError("MetricArtifact metric_ref 无效")
        _digest(self.definition_digest, "definition_digest")
        _text(self.unit, "unit")
        object.__setattr__(self, "sample_scope", _string_mapping(self.sample_scope, "sample_scope"))
        _digest(self.implementation_digest, "implementation_digest")
        sources = {
            _text(key, "source_artifact_digests.key"): _digest(
                value,
                f"source_artifact_digests.{key}",
            )
            for key, value in self.source_artifact_digests.items()
        }
        if not sources:
            raise MetricContractError("MetricArtifact 必须绑定源 Artifact")
        object.__setattr__(
            self,
            "source_artifact_digests",
            MappingProxyType(dict(sorted(sources.items()))),
        )
        if not self.values or any(not isinstance(item, Mapping) for item in self.values):
            raise MetricContractError("MetricArtifact values 必须是非空映射序列")
        if self.contract_version != METRIC_ARTIFACT_VERSION:
            raise MetricContractError("MetricArtifact 版本不受支持")
        if self.artifact_digest != typed_canonical_hash(self.payload()):
            raise MetricContractError("MetricArtifact digest 不一致")

    @property
    def metric_id(self) -> str:
        return self.metric_ref.rpartition("@")[0]

    @property
    def version(self) -> str:
        return self.metric_ref.rpartition("@")[2]

    def payload(self) -> dict[str, object]:
        return {
            "metric_ref": self.metric_ref,
            "metric_id": self.metric_id,
            "version": self.version,
            "definition_digest": self.definition_digest,
            "unit": self.unit,
            "sample_scope": dict(self.sample_scope),
            "implementation_digest": self.implementation_digest,
            "source_artifact_digests": dict(self.source_artifact_digests),
            "values": [dict(item) for item in self.values],
            "contract_version": self.contract_version,
        }

    @classmethod
    def build(
        cls,
        *,
        definition: MetricDefinition,
        sample_scope: Mapping[str, str],
        source_artifact_digests: Mapping[str, str],
        values: Sequence[Mapping[str, object]],
    ) -> "MetricArtifact":
        payload = {
            "metric_ref": definition.metric_ref,
            "metric_id": definition.metric_id,
            "version": definition.version,
            "definition_digest": definition.definition_digest,
            "unit": definition.unit,
            "sample_scope": dict(sorted(sample_scope.items())),
            "implementation_digest": definition.implementation_digest,
            "source_artifact_digests": dict(sorted(source_artifact_digests.items())),
            "values": [dict(item) for item in values],
            "contract_version": METRIC_ARTIFACT_VERSION,
        }
        return cls(
            payload["metric_ref"],
            payload["definition_digest"],
            payload["unit"],
            payload["sample_scope"],
            payload["implementation_digest"],
            payload["source_artifact_digests"],
            tuple(payload["values"]),
            typed_canonical_hash(payload),
        )


class MetricRegistry:
    def __init__(self, definitions: Sequence[MetricDefinition]) -> None:
        items = tuple(sorted(definitions, key=lambda item: item.metric_ref))
        by_ref = {item.metric_ref: item for item in items}
        if not items or len(by_ref) != len(items):
            raise MetricContractError("Metric registry 为空或存在重复 metric ref")
        self._definitions = MappingProxyType(by_ref)
        self.registry_digest = typed_canonical_hash(
            {"definitions": [item.payload() for item in items]}
        )

    def require(self, metric_ref: str) -> MetricDefinition:
        try:
            return self._definitions[metric_ref]
        except KeyError as exc:
            raise UnknownMetricError(f"未注册 metric: {metric_ref}") from exc

    @property
    def definitions(self) -> tuple[MetricDefinition, ...]:
        return tuple(self._definitions.values())

    def prove(
        self,
        metric_refs: Sequence[str],
        producers: Sequence[tuple[str, str, str]],
        result_tables: Sequence[object],
    ) -> tuple[MetricReachabilityProof, ...]:
        if not metric_refs or len(set(metric_refs)) != len(metric_refs):
            raise MetricContractError("metric refs 必须非空且唯一")
        proofs = []
        for metric_ref in metric_refs:
            definition = self.require(metric_ref)
            matches = tuple(
                producer
                for producer in producers
                if producer[2] == definition.input_artifact_type
            )
            if len(matches) != 1:
                raise UnreachableMetricError(
                    f"metric 产出端口必须唯一可达: {metric_ref}; matches={len(matches)}"
                )
            node_id, port, artifact_type = matches[0]
            table_matches = tuple(
                table
                for table in result_tables
                if getattr(table, "source_node_id", None) == node_id
                and getattr(table, "source_port", None) == port
                and getattr(table, "artifact_type", None) == artifact_type
                and getattr(table, "schema_id", None)
                == definition.result_schema_id
            )
            if len(table_matches) != 1:
                raise UnreachableMetricError(
                    "metric 必须唯一绑定 ResultSpec 中的指标表: "
                    f"{metric_ref}; matches={len(table_matches)}"
                )
            table = table_matches[0]
            result_binding = (
                str(getattr(table, "table_id")),
                str(getattr(table, "schema_id")),
                str(getattr(table, "path_prefix")),
            )
            proofs.append(
                MetricReachabilityProof.build(
                    definition,
                    matches[0],
                    result_binding,
                )
            )
        return tuple(sorted(proofs, key=lambda item: item.metric_ref))


def _implementation_digest(implementation_ref: str, modules: Sequence[str]) -> str:
    files: dict[str, str] = {}
    for module in sorted(set(modules)):
        spec = importlib.util.find_spec(module)
        if spec is None or spec.origin is None:
            raise MetricContractError(f"metric implementation module 不存在: {module}")
        path = Path(spec.origin)
        files[module] = hashlib.sha256(path.read_bytes()).hexdigest()
    return typed_canonical_hash(
        {"implementation_ref": implementation_ref, "module_files": files}
    )


def _definition(
    metric_id: str,
    *,
    artifact_type: str,
    unit: str,
    frequency: str,
    annualization: str,
    null_policy: str,
    direction: str,
    implementation_ref: str,
    modules: Sequence[str],
) -> MetricDefinition:
    result_schemas = {
        "data.columnar-bundle.v1": "data.columnar-bundle.metrics.v1",
        "research.minute-observation.v1": "research.minute-observation.metrics.v1",
        "research.minute-statistics.v1": "research.minute-statistics.v1",
    }
    return MetricDefinition.build(
        metric_id=metric_id,
        version="1.0.0",
        input_artifact_type=artifact_type,
        result_schema_id=result_schemas[artifact_type],
        output_schema={
            "metric_ref": "string",
            "value": "float64",
            "unit": "string",
            "sample_start": "string",
            "sample_end": "string",
            "sample_size": "int64",
            "status": "string",
        },
        unit=unit,
        frequency=frequency,
        annualization_policy=annualization,
        risk_free_rate_policy="not_applicable",
        null_policy=null_policy,
        direction=direction,
        implementation_ref=implementation_ref,
        implementation_digest=_implementation_digest(implementation_ref, modules),
    )


def compose_metric_registry(project_definitions: Sequence[MetricDefinition]) -> MetricRegistry:
    """只在当前计划或 Result 内组合项目指标，不改公共发现注册表。"""
    return MetricRegistry((
        *build_mainline_metric_registry().definitions,
        *project_definitions,
    ))


def build_mainline_metric_registry() -> MetricRegistry:
    minute_module = "research_pipeline.research.statistics.minute_profile"
    definitions = (
        _definition(
            "data.row_count",
            artifact_type="data.columnar-bundle.v1",
            unit="rows",
            frequency="snapshot",
            annualization="none",
            null_policy="forbid",
            direction="neutral",
            implementation_ref="dataset_manifest.row_count",
            modules=("research_pipeline.data_plane.snapshots",),
        ),
        _definition(
            "minute.row_count",
            artifact_type="research.minute-observation.v1",
            unit="rows",
            frequency="bounded_minute_window",
            annualization="none",
            null_policy="forbid",
            direction="neutral",
            implementation_ref="partitioned_minute_manifest.row_count",
            modules=(
                "research_pipeline.data_plane.partitioned_artifacts",
                "research_pipeline.runtime.adapters.minute_data",
            ),
        ),
        _definition(
            "statistics.adjusted_p",
            artifact_type="research.minute-statistics.v1",
            unit="probability",
            frequency="candidate_family",
            annualization="none",
            null_policy="forbid",
            direction="lower_is_better",
            implementation_ref="minute_statistics.adjusted_p_value",
            modules=(minute_module,),
        ),
    )
    registry = MetricRegistry(definitions)
    validate_public_semantic_inventory(
        "metric",
        (item.metric_ref for item in registry.definitions),
        builtin_identities=BUILTIN_METRIC_IDENTITIES,
        reviews=MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    )
    return registry


__all__ = [
    "BUILTIN_METRIC_IDENTITIES",
    "METRIC_ARTIFACT_VERSION",
    "METRIC_DEFINITION_VERSION",
    "METRIC_REACHABILITY_VERSION",
    "MetricArtifact",
    "MetricContractError",
    "MetricDefinition",
    "MetricReachabilityProof",
    "MetricRegistry",
    "UnknownMetricError",
    "UnreachableMetricError",
    "build_mainline_metric_registry",
    "compose_metric_registry",
]
