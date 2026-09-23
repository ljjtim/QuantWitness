"""从自包含 Result 生成并消费结构化 VerificationResult。"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Callable, Iterable, Mapping

import pyarrow as pa

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.minute_operator_contracts import (
    MINUTE_TARGET_PAYLOAD_SCHEMA_ID,
)
from research_pipeline.platform.causal_time import (
    CausalTimeContractError,
    CORE_FEATURE_TIME_COLUMNS,
    CORE_LABEL_TIME_COLUMNS,
    causal_time_key_columns,
    validate_feature_time_facts,
    validate_label_time_facts,
)
from research_pipeline.platform.metric_contracts import (
    MetricDefinition,
    compose_metric_registry,
)
from research_pipeline.platform.semantic_governance import (
    MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    validate_public_semantic_inventory,
)
from research_pipeline.platform.statistics_contracts import (
    MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID,
    MINUTE_STATISTICS_SPLIT_SCHEMA_ID,
)
from research_pipeline.results import (
    BAR_TCA_SCHEMA_IDS,
    CANONICAL_SIMULATION_SCHEMA_IDS,
    MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS,
    ResultMetric,
    ResultReference,
    ResultRunSummary,
    ResultContractError,
    ResultSnapshot,
    ResultStore,
    metrics_from_snapshot,
)
from research_pipeline.results.adjustment_snapshot import (
    verify_adjustment_anchor_result_tables,
    verify_adjustment_snapshot_result_table,
)

from .causal_uniqueness import verify_causal_key_uniqueness
from .errors import EvidenceContractError
from .facets import ArtifactIntegrityFacet, ReproducibilityFacet, require_sha256, strict_fields
from .minute_validity import MINUTE_VERIFIER_ALGORITHM_VERSIONS
from .result_financial_oracle import (
    FinancialOracleBudget,
    FinancialOracleResult,
    FinancialOracleSemanticContract,
    verify_result_financial_oracle,
)
from .result_disposition import require_result_consumable
from .validity import VALIDITY_GATE_IDS, assess_claim_policy
from .validity_recompute import (
    GATE_ALGORITHM_VERSIONS,
    VALIDITY_FACTS_PRODUCER_HASH,
    GateRecomputeRecord,
    build_validity_gate_input_hashes,
    load_claim_policy,
    recompute_gate_results,
    simulation_claim_ceiling_from_facts,
)


VERIFICATION_RESULT_VERSION = "research-verification-result-v3"
_VERIFICATION_CATALOG_VERSION = "research-result-verification-catalog-v1"
_VERIFICATION_LINEAGE_VERSION = "research-result-verification-lineage-v1"
_FORMAL_FEATURE_ARTIFACT_TYPES = frozenset({
    "research.feature-set.v1",
    "research.minute-features.v1",
})
_FORMAL_LABEL_ARTIFACT_TYPES = frozenset({
    "research.label.v1",
    "research.minute-labels.v1",
})
_ADJUSTMENT_SNAPSHOT_MAX_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class _ResultSemanticHandler:
    handler_id: str
    schema_ids: tuple[str, ...]
    path_bindings: tuple[tuple[str, str], ...] = ()
    phase: str = "dependency"
    operation: str = "dependency"
    schema_roles: tuple[tuple[str, str], ...] = ()
    artifact_types: tuple[str, ...] = ()
    required_support_paths: tuple[str, ...] = ()
    optional_support_paths: tuple[str, ...] = ()
    verifier_identity: str = ""

    def __post_init__(self) -> None:
        if not self.handler_id or not self.schema_ids or not self.verifier_identity:
            raise EvidenceContractError("Result semantic handler 声明不完整")
        if self.schema_roles and tuple(
            schema_id for _, schema_id in self.schema_roles
        ) != self.schema_ids:
            raise EvidenceContractError("Result semantic handler schema role 不闭合")
        if self.path_bindings and set(dict(self.path_bindings)) != set(self.schema_ids):
            raise EvidenceContractError("Result semantic handler path binding 不闭合")

    @property
    def schema_set_identity(self) -> str:
        members = (
            tuple(f"{schema_id}@{path}" for schema_id, path in self.path_bindings)
            if self.path_bindings
            else self.schema_ids
        )
        return f"{self.handler_id}:" + ",".join(sorted(members))

    @property
    def schema_by_role(self) -> dict[str, str]:
        return dict(self.schema_roles)

    @property
    def path_by_schema(self) -> dict[str, str]:
        return dict(self.path_bindings)


_BAR_TCA_HANDLER = _ResultSemanticHandler(
    "bar_tca",
    tuple(BAR_TCA_SCHEMA_IDS.values()),
    tuple(
        (schema_id, f"simulation/tca/{role}")
        for role, schema_id in BAR_TCA_SCHEMA_IDS.items()
    ),
    phase="financial_oracle",
    operation="financial_oracle",
    schema_roles=tuple(BAR_TCA_SCHEMA_IDS.items()),
    required_support_paths=(
        "simulation/result-contract/manifest.json",
        "simulation/result-contract/COMMITTED",
        "simulation/tca/manifest.json",
        "simulation/tca/COMMITTED",
        "simulation/tca/oracle-input.json",
    ),
    optional_support_paths=(
        "simulation/context.json",
        "simulation/daily-context.json",
    ),
    verifier_identity=(
        "result-semantic:financial_oracle:result-bundle-financial-oracle-v5"
    ),
)
_CANONICAL_SIMULATION_HANDLER = _ResultSemanticHandler(
    "canonical_simulation",
    tuple(CANONICAL_SIMULATION_SCHEMA_IDS.values()),
    schema_roles=tuple(CANONICAL_SIMULATION_SCHEMA_IDS.items()),
    verifier_identity=_BAR_TCA_HANDLER.verifier_identity,
)
_MINUTE_FINANCIAL_CONTEXT_HANDLER = _ResultSemanticHandler(
    "minute_financial_context",
    (
        MINUTE_TARGET_PAYLOAD_SCHEMA_ID,
        *MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS.values(),
    ),
    schema_roles=(
        ("target", MINUTE_TARGET_PAYLOAD_SCHEMA_ID),
        *MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS.items(),
    ),
    verifier_identity=_BAR_TCA_HANDLER.verifier_identity,
)
_MINUTE_STATISTICS_HANDLER = _ResultSemanticHandler(
    "minute_statistics",
    (
        MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID,
        MINUTE_STATISTICS_SPLIT_SCHEMA_ID,
    ),
    phase="gate_inputs",
    operation="minute_statistics",
    schema_roles=(
        ("observations", MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID),
        ("split_assignments", MINUTE_STATISTICS_SPLIT_SCHEMA_ID),
    ),
    verifier_identity="minute:statistics:verifier.minute-statistics.v2",
)
_ADJUSTMENT_HANDLER = _ResultSemanticHandler(
    "adjustment_snapshot",
    (
        "data.adjustment-factor-snapshot.payload.v1",
        "research.minute-features.pre-anchor.v1",
        "research.minute-labels.pre-anchor.v1",
    ),
    phase="pre_gate",
    operation="adjustment_snapshot",
    schema_roles=(
        ("snapshot", "data.adjustment-factor-snapshot.payload.v1"),
        ("feature_anchor", "research.minute-features.pre-anchor.v1"),
        ("label_anchor", "research.minute-labels.pre-anchor.v1"),
    ),
    artifact_types=("data.adjustment-factor-snapshot.v1",),
    verifier_identity=(
        "result-semantic:adjustment_snapshot:verifier.adjustment-snapshot.v1"
    ),
)
_RESULT_SEMANTIC_HANDLERS = (
    _BAR_TCA_HANDLER,
    _CANONICAL_SIMULATION_HANDLER,
    _MINUTE_FINANCIAL_CONTEXT_HANDLER,
    _MINUTE_STATISTICS_HANDLER,
    _ADJUSTMENT_HANDLER,
)


BUILTIN_RESULT_SCHEMA_IDENTITIES = frozenset({
    "data.adjustment-factor-snapshot.payload.v1",
    "data.columnar-bundle.metrics.v1",
    "research.bar-tca.daily.v1",
    "research.bar-tca.fills.v1",
    "research.bar-tca.orders.v1",
    "research.bar-tca.research.v1",
    "research.minute-features.pre-anchor.v1",
    "research.minute-labels.pre-anchor.v1",
    "research.minute-observation.metrics.v1",
    "research.minute-targets.payload.v1",
    "research.minute-financial-context.execution-bars.v1",
    "research.minute-financial-context.decision-benchmarks.v1",
    "research.minute-financial-context.execution-observations.v1",
    "research.minute-financial-context.settlement-events.v1",
    "research.minute-statistics.observations.v1",
    "research.minute-statistics.split-assignments.v1",
    "research.minute-statistics.v1",
    "research.simulation.cash.v1",
    "research.simulation.costs.v1",
    "research.simulation.fills.v1",
    "research.simulation.orders.v1",
    "research.simulation.positions.v1",
    "research.simulation.valuations.v1",
})
BUILTIN_RESULT_SCHEMA_SET_IDENTITIES = frozenset({
    "adjustment_snapshot:data.adjustment-factor-snapshot.payload.v1,"
    "research.minute-features.pre-anchor.v1,research.minute-labels.pre-anchor.v1",
    "bar_tca:research.bar-tca.daily.v1@simulation/tca/daily,"
    "research.bar-tca.fills.v1@simulation/tca/fills,"
    "research.bar-tca.orders.v1@simulation/tca/orders,"
    "research.bar-tca.research.v1@simulation/tca/research",
    "canonical_simulation:research.simulation.cash.v1,research.simulation.costs.v1,"
    "research.simulation.fills.v1,research.simulation.orders.v1,"
    "research.simulation.positions.v1,research.simulation.valuations.v1",
    "minute_statistics:research.minute-statistics.observations.v1,"
    "research.minute-statistics.split-assignments.v1",
    "minute_financial_context:research.minute-financial-context.decision-benchmarks.v1,"
    "research.minute-financial-context.execution-bars.v1,"
    "research.minute-financial-context.execution-observations.v1,"
    "research.minute-financial-context.settlement-events.v1,"
    "research.minute-targets.payload.v1",
})
BUILTIN_VERIFIER_IDENTITIES = frozenset({
    "default:data.pit:verifier.data-pit.v1",
    "default:financial.tradability:verifier.financial-tradability.v2",
    "default:label.split:verifier.label-split.v2",
    "default:search.holdout:verifier.search-holdout.v1",
    "default:statistics:verifier.statistics.v1",
    "minute:data.pit:verifier.minute-data-pit.v2",
    "minute:financial.tradability:verifier.minute-financial.v2",
    "minute:label.split:verifier.minute-label-split.v2",
    "minute:search.holdout:verifier.minute-trial-universe.v2",
    "minute:statistics:verifier.minute-statistics.v2",
    "result-semantic:adjustment_snapshot:verifier.adjustment-snapshot.v1",
    "result-semantic:financial_oracle:result-bundle-financial-oracle-v5",
})


def _semantic_handler(handler_id: str) -> _ResultSemanticHandler:
    matches = tuple(
        handler
        for handler in _RESULT_SEMANTIC_HANDLERS
        if handler.handler_id == handler_id
    )
    if len(matches) != 1:
        raise EvidenceContractError(
            f"Result semantic handler 必须唯一登记: {handler_id}"
        )
    return matches[0]


def financial_oracle_semantic_contract() -> FinancialOracleSemanticContract:
    """从权威 handler 注册表投影金融复核实际使用的 schema 和路径。"""

    bar_tca = _semantic_handler("bar_tca")
    canonical = _semantic_handler("canonical_simulation")
    minute_context = _semantic_handler("minute_financial_context")
    adjustment = _semantic_handler("adjustment_snapshot")
    if not bar_tca.required_support_paths or len(bar_tca.optional_support_paths) != 2:
        raise EvidenceContractError("金融复核 handler 的支持文件声明不完整")
    return FinancialOracleSemanticContract(
        canonical_schema_roles=canonical.schema_roles,
        bar_tca_schema_roles=bar_tca.schema_roles,
        bar_tca_path_bindings=bar_tca.path_bindings,
        minute_context_schema_roles=tuple(
            item for item in minute_context.schema_roles if item[0] != "target"
        ),
        minute_target_schema_id=minute_context.schema_by_role["target"],
        adjustment_snapshot_schema_id=adjustment.schema_by_role["snapshot"],
        required_control_paths=bar_tca.required_support_paths,
        minute_context_path=bar_tca.optional_support_paths[0],
        daily_etf_context_path=bar_tca.optional_support_paths[1],
    )


def validate_builtin_verification_semantics() -> None:
    """冻结 core 会主动解释的 Result schema 与 Verifier 算法身份。"""

    result_schemas = {
        *(
            schema_id
            for handler in _RESULT_SEMANTIC_HANDLERS
            for schema_id in handler.schema_ids
        ),
        *(
            item.result_schema_id
            for item in compose_metric_registry(()).definitions
        ),
    }
    validate_public_semantic_inventory(
        "result_schema",
        result_schemas,
        builtin_identities=BUILTIN_RESULT_SCHEMA_IDENTITIES,
        reviews=MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    )
    validate_public_semantic_inventory(
        "result_schema_set",
        (handler.schema_set_identity for handler in _RESULT_SEMANTIC_HANDLERS),
        builtin_identities=BUILTIN_RESULT_SCHEMA_SET_IDENTITIES,
        reviews=MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    )
    verifier_identities = {
        *(
            f"default:{gate_id}:{version}"
            for gate_id, version in GATE_ALGORITHM_VERSIONS.items()
        ),
        *(
            f"minute:{gate_id}:{version}"
            for gate_id, version in MINUTE_VERIFIER_ALGORITHM_VERSIONS.items()
        ),
        *(handler.verifier_identity for handler in _RESULT_SEMANTIC_HANDLERS),
    }
    validate_public_semantic_inventory(
        "verifier",
        verifier_identities,
        builtin_identities=BUILTIN_VERIFIER_IDENTITIES,
        reviews=MAINLINE_SEMANTIC_PROMOTION_REVIEWS,
    )


@dataclass(frozen=True)
class VerificationResult:
    result_reference: ResultReference
    run: ResultRunSummary
    source_package_hash: str
    source_plan_hash: str
    source_runtime_hash: str
    source_implementation_manifest_hash: str
    source_catalog_hash: str
    source_lineage_root: str
    policy_id: str
    policy_hash: str
    gate_records: tuple[GateRecomputeRecord, ...]
    integrity_status: str
    reproducibility_status: str
    validity_status: str
    claim_level: str
    claim_ceiling: str
    limitations: tuple[str, ...]
    research_cost_assumption: Mapping[str, object] | None
    project_verifier_identity: Mapping[str, object] | None
    project_verifier_outcome_hash: str | None
    verification_hash: str
    status: str = "pass"
    contract_version: str = VERIFICATION_RESULT_VERSION

    def __post_init__(self) -> None:
        if (
            self.status not in {"pass", "fail"}
            or self.contract_version != VERIFICATION_RESULT_VERSION
        ):
            raise EvidenceContractError("VerificationResult 状态或版本无效")
        if self.verification_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("VerificationResult hash 不一致")
        for field in (
            "source_package_hash", "source_plan_hash", "source_runtime_hash",
            "source_implementation_manifest_hash", "source_catalog_hash",
            "source_lineage_root", "policy_hash",
        ):
            require_sha256(getattr(self, field), field)
        if self.integrity_status != "pass" or self.reproducibility_status != "pass":
            raise EvidenceContractError(
                "VerificationResult 只能描述已完整读取且可复现的 Result"
            )
        if self.validity_status not in {"pass", "fail"}:
            raise EvidenceContractError("VerificationResult validity 状态无效")
        if (self.status == "pass") != (self.validity_status == "pass"):
            raise EvidenceContractError(
                "VerificationResult 状态必须如实反映 validity 结论"
            )
        policy = load_claim_policy(self.policy_id)
        if policy.policy_hash != self.policy_hash or policy.requested_level != self.claim_level:
            raise EvidenceContractError("VerificationResult policy 与 claim 不一致")
        gate_ids = tuple(item.gate_id for item in self.gate_records)
        if gate_ids != VALIDITY_GATE_IDS:
            raise EvidenceContractError("VerificationResult gate 记录不完整或未规范排序")
        algorithms = tuple(sorted(
            (item.gate_id, item.algorithm_version) for item in self.gate_records
        ))
        if algorithms not in {
            tuple(sorted(GATE_ALGORITHM_VERSIONS.items())),
            tuple(sorted(MINUTE_VERIFIER_ALGORITHM_VERSIONS.items())),
        }:
            raise EvidenceContractError("VerificationResult verifier 算法组不受支持")
        if tuple(sorted(set(self.limitations))) != self.limitations:
            raise EvidenceContractError("VerificationResult limitations 必须唯一并规范排序")
        if self.research_cost_assumption is not None:
            if not isinstance(self.research_cost_assumption, Mapping):
                raise EvidenceContractError("VerificationResult 研究费用假设无效")
            object.__setattr__(
                self,
                "research_cost_assumption",
                dict(self.research_cost_assumption),
            )
        if (self.project_verifier_identity is None) != (
            self.project_verifier_outcome_hash is None
        ):
            raise EvidenceContractError("VerificationResult 项目 Verifier 绑定不闭合")
        if self.project_verifier_identity is not None:
            if not isinstance(self.project_verifier_identity, Mapping):
                raise EvidenceContractError("VerificationResult 项目 Verifier identity 无效")
            require_sha256(
                self.project_verifier_outcome_hash,
                "project_verifier_outcome_hash",
            )
            object.__setattr__(
                self,
                "project_verifier_identity",
                dict(self.project_verifier_identity),
            )

    def payload(self) -> dict[str, object]:
        payload = {
            "result_reference": self.result_reference.to_dict(),
            "run": self.run.to_dict(),
            "source_package_hash": self.source_package_hash,
            "source_plan_hash": self.source_plan_hash,
            "source_runtime_hash": self.source_runtime_hash,
            "source_implementation_manifest_hash": self.source_implementation_manifest_hash,
            "source_catalog_hash": self.source_catalog_hash,
            "source_lineage_root": self.source_lineage_root,
            "policy_id": self.policy_id,
            "policy_hash": self.policy_hash,
            "gate_records": [item.to_dict() for item in self.gate_records],
            "integrity_status": self.integrity_status,
            "reproducibility_status": self.reproducibility_status,
            "validity_status": self.validity_status,
            "claim_level": self.claim_level,
            "claim_ceiling": self.claim_ceiling,
            "limitations": list(self.limitations),
            "research_cost_assumption": self.research_cost_assumption,
            "status": self.status,
            "contract_version": self.contract_version,
        }
        if self.project_verifier_identity is not None:
            payload["project_verifier_identity"] = dict(self.project_verifier_identity)
            payload["project_verifier_outcome_hash"] = self.project_verifier_outcome_hash
        return payload

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "verification_hash": self.verification_hash}

    @classmethod
    def build(cls, **values: object) -> "VerificationResult":
        status = "pass" if values["validity_status"] == "pass" else "fail"
        payload = {
            "result_reference": values["result_reference"].to_dict(),
            "run": values["run"].to_dict(),
            "source_package_hash": values["source_package_hash"],
            "source_plan_hash": values["source_plan_hash"],
            "source_runtime_hash": values["source_runtime_hash"],
            "source_implementation_manifest_hash": values[
                "source_implementation_manifest_hash"
            ],
            "source_catalog_hash": values["source_catalog_hash"],
            "source_lineage_root": values["source_lineage_root"],
            "policy_id": values["policy_id"],
            "policy_hash": values["policy_hash"],
            "gate_records": [item.to_dict() for item in values["gate_records"]],
            "integrity_status": values["integrity_status"],
            "reproducibility_status": values["reproducibility_status"],
            "validity_status": values["validity_status"],
            "claim_level": values["claim_level"],
            "claim_ceiling": values["claim_ceiling"],
            "limitations": list(values["limitations"]),
            "research_cost_assumption": values["research_cost_assumption"],
            "status": status,
            "contract_version": VERIFICATION_RESULT_VERSION,
        }
        if values["project_verifier_identity"] is not None:
            payload["project_verifier_identity"] = values["project_verifier_identity"]
            payload["project_verifier_outcome_hash"] = values["project_verifier_outcome_hash"]
        return cls(
            **values,
            verification_hash=typed_canonical_hash(payload),
            status=status,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "VerificationResult":
        expected = {
            "result_reference", "run", "source_package_hash", "source_plan_hash",
            "source_runtime_hash", "source_implementation_manifest_hash",
            "source_catalog_hash", "source_lineage_root", "policy_id", "policy_hash",
            "gate_records", "integrity_status", "reproducibility_status",
            "validity_status", "claim_level", "claim_ceiling", "limitations",
            "research_cost_assumption",
            "verification_hash", "status", "contract_version",
        }
        if "project_verifier_identity" in payload or "project_verifier_outcome_hash" in payload:
            expected.update({"project_verifier_identity", "project_verifier_outcome_hash"})
        strict_fields(payload, expected, "VerificationResult")
        if not isinstance(payload["result_reference"], Mapping) or not isinstance(payload["run"], Mapping):
            raise EvidenceContractError("VerificationResult 身份字段无效")
        records = payload["gate_records"]
        limitations = payload["limitations"]
        if (
            not isinstance(records, list)
            or any(not isinstance(item, Mapping) for item in records)
            or not isinstance(limitations, list)
            or any(not isinstance(item, str) for item in limitations)
            or not (
                payload["research_cost_assumption"] is None
                or isinstance(payload["research_cost_assumption"], Mapping)
            )
        ):
            raise EvidenceContractError("VerificationResult gate_records/limitations 无效")
        return cls(
            result_reference=ResultReference.from_dict(payload["result_reference"]),
            run=ResultRunSummary.from_dict(payload["run"]),
            source_package_hash=str(payload["source_package_hash"]),
            source_plan_hash=str(payload["source_plan_hash"]),
            source_runtime_hash=str(payload["source_runtime_hash"]),
            source_implementation_manifest_hash=str(
                payload["source_implementation_manifest_hash"]
            ),
            source_catalog_hash=str(payload["source_catalog_hash"]),
            source_lineage_root=str(payload["source_lineage_root"]),
            policy_id=str(payload["policy_id"]),
            policy_hash=str(payload["policy_hash"]),
            gate_records=tuple(GateRecomputeRecord.from_dict(item) for item in records),
            integrity_status=str(payload["integrity_status"]),
            reproducibility_status=str(payload["reproducibility_status"]),
            validity_status=str(payload["validity_status"]),
            claim_level=str(payload["claim_level"]),
            claim_ceiling=str(payload["claim_ceiling"]),
            limitations=tuple(str(item) for item in limitations),
            research_cost_assumption=(
                None
                if payload["research_cost_assumption"] is None
                else dict(payload["research_cost_assumption"])
            ),
            project_verifier_identity=(
                None
                if payload.get("project_verifier_identity") is None
                else dict(payload["project_verifier_identity"])
            ),
            project_verifier_outcome_hash=(
                None
                if payload.get("project_verifier_outcome_hash") is None
                else str(payload["project_verifier_outcome_hash"])
            ),
            verification_hash=str(payload["verification_hash"]),
            status=str(payload["status"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class VerifiedResultContext:
    verification: VerificationResult
    snapshot: ResultSnapshot
    metrics: tuple[ResultMetric, ...]


@dataclass(frozen=True)
class VerificationReportModel:
    verification_hash: str
    result_id: str
    integrity_status: str
    reproducibility_status: str
    validity_status: str
    claim_level: str
    claim_ceiling: str
    limitations: tuple[str, ...]
    research_cost_assumption: Mapping[str, object] | None
    metrics: tuple[ResultMetric, ...]


@dataclass(frozen=True)
class VerificationComparison:
    comparison_scope: str
    scope_note: str
    comparable: bool
    reason_codes: tuple[str, ...]
    same_result: bool
    same_plan: bool
    same_package: bool
    same_implementation: bool
    metric_comparisons: tuple["_MetricComparison", ...]


@dataclass(frozen=True)
class _MetricComparison:
    metric_ref: str
    unit: str
    direction: str
    sample_start: str
    sample_end: str
    sample_size: int
    status: str
    left_value: float
    right_value: float
    delta: float


@dataclass(frozen=True)
class _ArrowBatchSource:
    """可重复逐批读取的 Arrow 表引用。"""

    schema: pa.Schema
    row_count: int
    iter_batches: Callable[[], Iterable[pa.RecordBatch]]


@dataclass(frozen=True)
class _SnapshotBatchTableView:
    """为现有逐批校验器提供不物化整表的最小 Arrow 表接口。"""

    snapshot: ResultSnapshot
    schema_id: str
    schema: pa.Schema
    num_rows: int

    @property
    def column_names(self) -> list[str]:
        return self.schema.names

    def to_batches(self, *, max_chunksize: int | None = None):
        return _snapshot_batches(
            self.snapshot,
            self.schema_id,
            batch_size=max_chunksize or 8_192,
        )


def _arrow_table_rows(table: pa.Table):
    """逐批投影 Result 表，避免为独立重算再创建整表 Python 行列表。"""

    for batch in table.to_batches(max_chunksize=8_192):
        names = tuple(batch.schema.names)
        columns = tuple(batch.column(index) for index in range(len(names)))
        for row_index in range(batch.num_rows):
            yield {
                name: column[row_index].as_py()
                for name, column in zip(names, columns, strict=True)
            }


def _snapshot_table(snapshot: ResultSnapshot, schema_id: str) -> pa.Table:
    tables = getattr(snapshot, "tables", {})
    if schema_id in tables:
        return tables[schema_id]
    try:
        return snapshot.read_table(schema_id)
    except (AttributeError, ResultContractError) as exc:
        raise EvidenceContractError(
            f"Result 正式表无法读取: {schema_id}"
        ) from exc


def _snapshot_small_table(
    snapshot: ResultSnapshot,
    schema_id: str,
    *,
    max_uncompressed_bytes: int,
) -> pa.Table:
    """只物化明确受支持的小型控制表。"""

    tables = getattr(snapshot, "tables", {})
    if schema_id in tables:
        table = tables[schema_id]
        if table.nbytes > max_uncompressed_bytes:
            raise EvidenceContractError(
                f"Result 控制表超出整表读取支持包络: {schema_id}"
            )
        return table
    try:
        return snapshot.read_table(
            schema_id,
            max_uncompressed_bytes=max_uncompressed_bytes,
        )
    except (AttributeError, ResultContractError) as exc:
        raise EvidenceContractError(
            f"Result 正式控制表无法读取: {schema_id}"
        ) from exc


def _snapshot_batches(
    snapshot: ResultSnapshot,
    schema_id: str,
    *,
    columns: tuple[str, ...] | None = None,
    batch_size: int = 8_192,
):
    """兼容真实 lazy snapshot 与测试内存快照的统一逐批入口。"""

    tables = getattr(snapshot, "tables", {})
    if schema_id in tables:
        table = tables[schema_id]
        if columns is not None:
            table = table.select(columns)
        yield from table.to_batches(max_chunksize=batch_size)
        return
    try:
        yield from snapshot.iter_table_batches(
            schema_id,
            columns=columns,
            batch_size=batch_size,
        )
    except (AttributeError, ResultContractError) as exc:
        raise EvidenceContractError(
            f"Result 正式表无法分区读取: {schema_id}"
        ) from exc


def _snapshot_schema(snapshot: ResultSnapshot, schema_id: str) -> pa.Schema:
    tables = getattr(snapshot, "tables", {})
    if schema_id in tables:
        return tables[schema_id].schema
    try:
        return snapshot.table_schema(schema_id)
    except (AttributeError, ResultContractError) as exc:
        raise EvidenceContractError(
            f"Result 正式表 schema 无法读取: {schema_id}"
        ) from exc


def _snapshot_rows(
    snapshot: ResultSnapshot,
    schema_id: str,
    *,
    columns: tuple[str, ...] | None = None,
):
    for batch in _snapshot_batches(snapshot, schema_id, columns=columns):
        names = tuple(batch.schema.names)
        arrays = tuple(batch.column(index) for index in range(len(names)))
        for row_index in range(batch.num_rows):
            yield {
                name: array[row_index].as_py()
                for name, array in zip(names, arrays, strict=True)
            }


def _snapshot_batch_source(
    snapshot: ResultSnapshot,
    schema_id: str,
) -> _ArrowBatchSource:
    try:
        row_count = snapshot.table_row_count(schema_id)
    except AttributeError:
        tables = getattr(snapshot, "tables", {})
        try:
            row_count = tables[schema_id].num_rows
        except KeyError as exc:
            raise EvidenceContractError(
                f"Result 正式表行数无法读取: {schema_id}"
            ) from exc
    except ResultContractError as exc:
        raise EvidenceContractError(
            f"Result 正式表行数无法读取: {schema_id}"
        ) from exc
    return _ArrowBatchSource(
        schema=_snapshot_schema(snapshot, schema_id),
        row_count=row_count,
        iter_batches=lambda: _snapshot_batches(snapshot, schema_id),
    )


def _arrow_batch_source(table: pa.Table | _ArrowBatchSource) -> _ArrowBatchSource:
    if isinstance(table, _ArrowBatchSource):
        return table
    return _ArrowBatchSource(
        schema=table.schema,
        row_count=table.num_rows,
        iter_batches=lambda: table.to_batches(max_chunksize=65_536),
    )


def _snapshot_batch_table_view(
    snapshot: ResultSnapshot,
    schema_id: str,
) -> _SnapshotBatchTableView:
    try:
        row_count = snapshot.table_row_count(schema_id)
    except AttributeError:
        tables = getattr(snapshot, "tables", {})
        try:
            row_count = tables[schema_id].num_rows
        except KeyError as exc:
            raise EvidenceContractError(
                f"Result 正式表行数无法读取: {schema_id}"
            ) from exc
    except ResultContractError as exc:
        raise EvidenceContractError(
            f"Result 正式表行数无法读取: {schema_id}"
        ) from exc
    return _SnapshotBatchTableView(
        snapshot=snapshot,
        schema_id=schema_id,
        schema=_snapshot_schema(snapshot, schema_id),
        num_rows=row_count,
    )


def verify_result(
    result: str | Path,
    *,
    result_store: str | Path,
    output: str | Path | None = None,
    financial_oracle_budget: FinancialOracleBudget | None = None,
    verifier_bundle: str | Path | None = None,
) -> VerifiedResultContext:
    """只从 ResultStore 独立重算门禁并生成 VerificationResult。"""

    validate_builtin_verification_semantics()
    store = ResultStore(result_store, create=False)
    manifest = store.inspect_directory(result)
    snapshot = _load_snapshot(store, manifest)
    project_verifier = None
    verifier_identity = snapshot.bundle.verification.verifier_identity
    if verifier_identity is not None:
        if verifier_bundle is None:
            raise EvidenceContractError(
                "Result 声明了项目 Verifier，verify 必须显式提供 bundle"
            )
        from research_pipeline.evidence.project_verifier_runtime import execute_project_verifier
        project_verifier = execute_project_verifier(
            bundle_path=verifier_bundle,
            snapshot=snapshot,
            expected_identity=verifier_identity,
            scratch_root=(
                None if financial_oracle_budget is None
                else financial_oracle_budget.scratch_root
            ),
        )
    elif verifier_bundle is not None:
        raise EvidenceContractError("Result 未冻结项目 Verifier identity")
    _verify_adjustment_snapshot_tables(
        snapshot,
        handler=_semantic_handler("adjustment_snapshot"),
    )
    _verify_causal_time_result_tables(snapshot, budget=financial_oracle_budget)
    bundle = snapshot.bundle
    closure = bundle.verification
    if closure.validity_producer_hash != VALIDITY_FACTS_PRODUCER_HASH:
        raise EvidenceContractError("Result validity facts 不是当前受信验证节点产物")
    try:
        facts = json.loads(snapshot.support_bytes[closure.validity_source_path])
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError("Result validity facts 无法读取") from exc
    if not isinstance(facts, Mapping):
        raise EvidenceContractError("Result validity facts 必须是对象")
    _verify_input_claim_lineage(bundle, facts)
    research_cost_assumption = _research_cost_assumption_from_facts(facts)

    policy = load_claim_policy(closure.policy_id)
    catalog_hash = _verification_catalog_hash(bundle)
    lineage_root = _verification_lineage_root(bundle)
    input_hashes = build_validity_gate_input_hashes(
        facts_artifact_hash=closure.validity_content_hash,
        catalog_hash=catalog_hash,
        lineage_root=lineage_root,
        policy_hash=policy.policy_hash,
    )
    financial_oracle = verify_result_financial_oracle(
        store=store,
        bundle=bundle,
        snapshot=snapshot,
        budget=financial_oracle_budget,
        semantic_contract=financial_oracle_semantic_contract(),
    )
    if financial_oracle is not None:
        # 金融 oracle 会独立变更；必须让检查版本/结果改变 VerificationResult 身份。
        input_hashes = tuple(sorted({
            *input_hashes,
            financial_oracle.oracle_hash,
        }))
    if project_verifier is not None:
        input_hashes = tuple(sorted({*input_hashes, project_verifier.outcome_hash}))
    declared_schema_ids = {table.schema_id for table in bundle.tables}
    minute_statistics = _semantic_handler("minute_statistics").schema_by_role
    gates = recompute_gate_results(
        facts,
        input_hashes=input_hashes,
        bar_tca_expectations=(
            None if financial_oracle is None else financial_oracle.bar_tca_expectations
        ),
        minute_statistics_observations=(
            ()
            if minute_statistics["observations"] not in declared_schema_ids
            else _snapshot_rows(
                snapshot,
                minute_statistics["observations"],
            )
        ),
        minute_statistics_split_assignments=(
            ()
            if minute_statistics["split_assignments"] not in declared_schema_ids
            else _snapshot_rows(snapshot, minute_statistics["split_assignments"])
        ),
    )
    integrity = ArtifactIntegrityFacet.build(catalog_hash)
    reproducibility = ReproducibilityFacet.build(
        plan_hash=bundle.plan_hash,
        snapshot_hashes=tuple(
            item.physical_snapshot_id for item in bundle.input_revisions
        ),
        lineage_hash=lineage_root,
        runtime_hash=bundle.runtime_hash,
        implementation_manifest_hash=bundle.implementation_manifest_hash,
        deterministic=closure.run.mode == "deterministic_serial",
    )
    assessment = assess_claim_policy(
        policy=policy,
        gate_results=gates,
        integrity=integrity,
        reproducibility=reproducibility,
        external_claim_ceiling=simulation_claim_ceiling_from_facts(facts),
    )
    algorithms = (
        MINUTE_VERIFIER_ALGORITHM_VERSIONS
        if isinstance(facts.get("statistics"), Mapping)
        and isinstance(facts["statistics"].get("minute_intraday"), Mapping)
        else GATE_ALGORITHM_VERSIONS
    )
    records = tuple(
        GateRecomputeRecord(
            gate.gate_id,
            algorithms[gate.gate_id],
            gate.input_hashes,
            gate.status,
            gate.findings,
            gate.result_hash,
        )
        for gate in gates
    )
    validity_status = assessment.validity.status
    limitations = assessment.claim.limitations
    if project_verifier is not None and project_verifier.status != "pass":
        validity_status = "fail"
        limitations = tuple(sorted({*limitations, *project_verifier.findings}))
    verification = VerificationResult.build(
        result_reference=ResultReference.build(bundle),
        run=closure.run,
        source_package_hash=bundle.package_hash,
        source_plan_hash=bundle.plan_hash,
        source_runtime_hash=bundle.runtime_hash,
        source_implementation_manifest_hash=bundle.implementation_manifest_hash,
        source_catalog_hash=catalog_hash,
        source_lineage_root=lineage_root,
        policy_id=closure.policy_id,
        policy_hash=policy.policy_hash,
        gate_records=records,
        integrity_status=integrity.status,
        reproducibility_status=reproducibility.status,
        validity_status=validity_status,
        claim_level=assessment.claim.claim_level,
        claim_ceiling=assessment.validity.claim_ceiling,
        limitations=limitations,
        research_cost_assumption=research_cost_assumption,
        project_verifier_identity=(
            None if project_verifier is None else dict(project_verifier.verifier_identity)
        ),
        project_verifier_outcome_hash=(
            None if project_verifier is None else project_verifier.outcome_hash
        ),
    )
    _validate_result_binding(
        verification,
        snapshot,
        financial_oracle=financial_oracle,
    )
    if output is not None:
        write_verification_result(verification, output)
    return VerifiedResultContext(verification, snapshot, metrics_from_snapshot(snapshot))


def _verify_input_claim_lineage(bundle, facts: Mapping[str, object]) -> None:
    """用 Result 正式边界对账 Runtime 生成的输入上限，不接受自报漂移。"""

    data_pit = facts.get("data_pit")
    if not isinstance(data_pit, Mapping):
        raise EvidenceContractError("Result validity facts 缺少 data_pit")
    if (
        data_pit.get("consumed_request_ids")
        != list(bundle.formal_input_request_ids)
        or data_pit.get("input_claim_ceilings")
        != dict(bundle.input_claim_ceilings)
        or data_pit.get("effective_claim_ceiling")
        != bundle.effective_input_claim_ceiling
    ):
        raise EvidenceContractError("Result 与 validity 的输入 claim lineage 不一致")


def _research_cost_assumption_from_facts(
    facts: Mapping[str, object],
) -> dict[str, object] | None:
    financial = facts.get("financial_tradability")
    if not isinstance(financial, Mapping):
        return None
    value = financial.get("research_cost_assumption")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EvidenceContractError("Result 的 research_cost_assumption 无效")
    return dict(value)


def write_verification_result(result: VerificationResult, destination: str | Path) -> Path:
    path = Path(destination).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(result.to_dict()))
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise EvidenceContractError("VerificationResult 输出必须不存在") from exc
    return path


def load_verified_result_context(
    verification_result: str | Path,
    *,
    result_store: str | Path,
) -> VerifiedResultContext:
    try:
        raw = Path(verification_result).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError("VerificationResult 无法读取") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceContractError("VerificationResult 必须是对象")
    verification = VerificationResult.from_dict(payload)
    if raw != canonical_json(verification.to_dict()):
        raise EvidenceContractError("VerificationResult 不是规范 JSON")
    store = ResultStore(result_store, create=False)
    reference = verification.result_reference
    bundle = store.inspect_by_identity(
        project_id=reference.project_id,
        run_id=reference.run_id,
        result_id=reference.result_id,
    )
    schema_ids = _verified_consumer_schema_ids(bundle)
    snapshot = store.load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        verify_schema_ids=schema_ids,
        support_paths=(bundle.verification.validity_source_path,),
        verify_all_files=False,
    )
    _validate_verified_consumer_binding(verification, snapshot)
    return VerifiedResultContext(
        verification,
        snapshot,
        metrics_from_snapshot(snapshot),
    )


def _verified_consumer_schema_ids(bundle) -> tuple[str, ...]:
    """只加载报告与比较实际消费的小型正式表。"""

    return tuple(sorted({
        proof.result_schema_id for proof in bundle.metric_proofs
    }))


def _validate_verified_consumer_binding(
    verification: VerificationResult,
    snapshot: ResultSnapshot,
) -> None:
    """复核 VerificationResult 自身与 Result 根身份，不重跑金融或统计 oracle。"""

    bundle = snapshot.bundle
    policy = load_claim_policy(bundle.verification.policy_id)
    try:
        facts = json.loads(
            snapshot.support_bytes[bundle.verification.validity_source_path]
        )
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError("Result validity facts 无法读取") from exc
    if not isinstance(facts, Mapping):
        raise EvidenceContractError("Result validity facts 必须是对象")
    expected_cost_assumption = _research_cost_assumption_from_facts(facts)
    expected_inputs = set(
        build_validity_gate_input_hashes(
            facts_artifact_hash=bundle.verification.validity_content_hash,
            catalog_hash=_verification_catalog_hash(bundle),
            lineage_root=_verification_lineage_root(bundle),
            policy_hash=policy.policy_hash,
        )
    )
    record_inputs = {record.input_hashes for record in verification.gate_records}
    if (
        verification.result_reference != ResultReference.build(bundle)
        or verification.run != bundle.verification.run
        or verification.source_package_hash != bundle.package_hash
        or verification.source_plan_hash != bundle.plan_hash
        or verification.source_runtime_hash != bundle.runtime_hash
        or verification.source_implementation_manifest_hash
        != bundle.implementation_manifest_hash
        or verification.source_catalog_hash != _verification_catalog_hash(bundle)
        or verification.source_lineage_root != _verification_lineage_root(bundle)
        or verification.policy_id != bundle.verification.policy_id
        or verification.policy_hash != policy.policy_hash
        or verification.project_verifier_identity != bundle.verification.verifier_identity
        or (
            verification.project_verifier_outcome_hash is not None
            and verification.project_verifier_outcome_hash not in next(iter(record_inputs), ())
        )
        or verification.research_cost_assumption != expected_cost_assumption
        or len(record_inputs) != 1
        or not expected_inputs <= set(next(iter(record_inputs), ()))
    ):
        raise EvidenceContractError(
            "VerificationResult 与 Result 的根身份或已验证输入绑定不一致"
        )


def _load_snapshot(
    store: ResultStore,
    bundle,
    *,
    include_verification_material: bool = True,
) -> ResultSnapshot:
    support_paths: set[str] = set()
    if include_verification_material:
        support_paths.add(bundle.verification.validity_source_path)
        identity = bundle.verification.verifier_identity
        if identity is not None:
            allowed_types = set(identity["authorized_support_artifact_types"])
            support_paths.update(
                item.source_path
                for item in bundle.support_files
                if item.artifact_type in allowed_types
            )
    financial_handler = _semantic_handler("bar_tca")
    if set(financial_handler.schema_ids) & {
        table.schema_id for table in bundle.tables
    }:
        if include_verification_material:
            support_paths.update(financial_handler.required_support_paths)
            declared_support = {
                item.source_path for item in bundle.support_files
            }
            support_paths.update(
                path
                for path in financial_handler.optional_support_paths
                if path in declared_support
            )
    return store.load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        support_paths=tuple(sorted(support_paths)),
    )


def _verify_adjustment_snapshot_tables(
    snapshot: ResultSnapshot,
    *,
    handler: _ResultSemanticHandler | None = None,
) -> None:
    """只从 ResultStore 复核实际消费的 PIT 快照，不回读运行目录或数据库。"""

    handler = handler or _semantic_handler("adjustment_snapshot")
    schemas = handler.schema_by_role
    if set(schemas) != {"snapshot", "feature_anchor", "label_anchor"}:
        raise EvidenceContractError("复权 Result semantic handler 角色无效")
    if len(handler.artifact_types) != 1:
        raise EvidenceContractError("复权 Result semantic handler Artifact 无效")
    snapshot_schema = schemas["snapshot"]
    feature_anchor_schema = schemas["feature_anchor"]
    label_anchor_schema = schemas["label_anchor"]

    manifests = tuple(
        item
        for item in snapshot.bundle.tables
        if item.artifact_type == handler.artifact_types[0]
    )
    if not manifests:
        return
    if (
        len(manifests) != 1
        or manifests[0].schema_id != snapshot_schema
    ):
        raise EvidenceContractError("Result 的 PIT 复权快照表身份不唯一")
    table = _snapshot_small_table(
        snapshot,
        snapshot_schema,
        max_uncompressed_bytes=_ADJUSTMENT_SNAPSHOT_MAX_BYTES,
    )
    try:
        verify_adjustment_snapshot_result_table(table)
    except ValueError as exc:
        raise EvidenceContractError("Result 的 PIT 复权快照完整载荷无效") from exc
    anchor_manifests = {
        item.schema_id: item
        for item in snapshot.bundle.tables
        if item.schema_id in {
            feature_anchor_schema,
            label_anchor_schema,
        }
    }
    if not anchor_manifests:
        return
    if set(anchor_manifests) != {
        feature_anchor_schema,
        label_anchor_schema,
    }:
        raise EvidenceContractError("pre 复权 Result 缺少 Feature 或 Label 锚点表")
    try:
        verify_adjustment_anchor_result_tables(
            table,
            _snapshot_batch_table_view(
                snapshot,
                feature_anchor_schema,
            ),
            _snapshot_batch_table_view(
                snapshot,
                label_anchor_schema,
            ),
        )
    except (EvidenceContractError, ValueError) as exc:
        raise EvidenceContractError("Result 的 pre 逐决策锚点无效") from exc


def _verification_catalog_hash(bundle) -> str:
    return typed_canonical_hash({
        "contract_version": _VERIFICATION_CATALOG_VERSION,
        "tables": [item.table_manifest_hash for item in bundle.tables],
        "support_files": [item.to_dict() for item in bundle.support_files],
    })


def _verification_lineage_root(bundle) -> str:
    return typed_canonical_hash({
        "contract_version": _VERIFICATION_LINEAGE_VERSION,
        "project_id": bundle.project_id,
        "run_id": bundle.run_id,
        "package_hash": bundle.package_hash,
        "plan_hash": bundle.plan_hash,
        "catalog_hashes": dict(bundle.catalog_hashes),
        "input_revisions": [item.to_dict() for item in bundle.input_revisions],
        "runtime_hash": bundle.runtime_hash,
        "implementation_manifest_hash": bundle.implementation_manifest_hash,
        "backend_fidelity_hash": bundle.backend_fidelity_hash,
        "result_spec_hash": bundle.result_spec.result_spec_hash,
        "metric_proofs": [item.proof_digest for item in bundle.metric_proofs],
        "tables": [item.table_manifest_hash for item in bundle.tables],
        "validity_content_hash": bundle.verification.validity_content_hash,
        "policy_id": bundle.verification.policy_id,
    })


def _validate_result_binding(
    verification: VerificationResult,
    snapshot: ResultSnapshot,
    *,
    financial_oracle: FinancialOracleResult | None,
) -> None:
    bundle = snapshot.bundle
    reference = verification.result_reference
    expected_inputs = build_validity_gate_input_hashes(
        facts_artifact_hash=bundle.verification.validity_content_hash,
        catalog_hash=_verification_catalog_hash(bundle),
        lineage_root=_verification_lineage_root(bundle),
        policy_hash=load_claim_policy(bundle.verification.policy_id).policy_hash,
    )
    if financial_oracle is not None:
        expected_inputs = tuple(sorted({
            *expected_inputs,
            financial_oracle.oracle_hash,
        }))
    if verification.project_verifier_outcome_hash is not None:
        expected_inputs = tuple(sorted({
            *expected_inputs,
            verification.project_verifier_outcome_hash,
        }))
    if (
        (reference.project_id, reference.run_id, reference.result_id)
        != (bundle.project_id, bundle.run_id, bundle.result_id)
        or verification.run != bundle.verification.run
        or verification.source_package_hash != bundle.package_hash
        or verification.source_plan_hash != bundle.plan_hash
        or verification.source_runtime_hash != bundle.runtime_hash
        or verification.source_implementation_manifest_hash
        != bundle.implementation_manifest_hash
        or verification.source_catalog_hash != _verification_catalog_hash(bundle)
        or verification.source_lineage_root != _verification_lineage_root(bundle)
        or verification.policy_id != bundle.verification.policy_id
        or verification.project_verifier_identity != bundle.verification.verifier_identity
        or any(record.input_hashes != expected_inputs for record in verification.gate_records)
    ):
        raise EvidenceContractError(
            "VerificationResult 与 Result 的 package/run/plan/runtime/implementation 绑定不一致"
        )


def build_verification_report(context: VerifiedResultContext) -> VerificationReportModel:
    verification = context.verification
    return VerificationReportModel(
        verification.verification_hash,
        context.snapshot.bundle.result_id,
        verification.integrity_status,
        verification.reproducibility_status,
        verification.validity_status,
        verification.claim_level,
        verification.claim_ceiling,
        verification.limitations,
        verification.research_cost_assumption,
        context.metrics,
    )


def render_verification_report(context: VerifiedResultContext) -> str:
    require_result_consumable(
        context.snapshot.bundle.result_id,
        consumer="report",
    )
    report = build_verification_report(context)
    metric_lines = [
        (
            f"- `{item.metric_ref}`：{item.value} {item.unit}；"
            f"样本 {item.sample_start} 至 {item.sample_end}，n={item.sample_size}，"
            f"状态 `{item.status}`"
        )
        for item in report.metrics
    ]
    limitations = "、".join(report.limitations) if report.limitations else "无"
    cost_lines = _render_research_cost_assumption(report.research_cost_assumption)
    return "\n".join((
        "# 研究验证报告",
        "",
        f"- Verification：`{report.verification_hash}`",
        f"- Result：`{report.result_id}`",
        f"- Integrity / Reproducibility / Validity：{report.integrity_status} / "
        f"{report.reproducibility_status} / {report.validity_status}",
        f"- Claim：`{report.claim_level}`（上限 `{report.claim_ceiling}`）",
        f"- 限制：{limitations}",
        *cost_lines,
        "",
        "## 正式指标",
        "",
        *metric_lines,
    ))


def _render_research_cost_assumption(
    assumption: Mapping[str, object] | None,
) -> tuple[str, ...]:
    if assumption is None:
        return ()
    return (
        "- A 股成本口径：研究假设（不是历史真实费率）；"
        f"适用 {assumption['applicable_start']} 至 {assumption['applicable_end']}；"
        f"佣金 {assumption['commission_ppm']} ppm，"
        f"最低佣金 {assumption['min_commission_units']} 分，"
        f"卖出税 {assumption['sell_tax_ppm']} ppm，"
        f"过户费 {assumption['transfer_fee_ppm']} ppm，"
        f"每股滑点 {assumption['slippage_per_share_units']} 分。",
    )


def _verify_causal_time_result_tables(
    snapshot: ResultSnapshot, *, budget: FinancialOracleBudget | None = None
) -> None:
    """从 Result 的正式逐行表复核因果时间与行键，不相信 validity 摘要。"""

    manifests = tuple(
        item
        for item in snapshot.bundle.tables
        if item.artifact_type
        in _FORMAL_FEATURE_ARTIFACT_TYPES | _FORMAL_LABEL_ARTIFACT_TYPES
    )
    if not manifests:
        return
    for manifest in manifests:
        is_feature = manifest.artifact_type in _FORMAL_FEATURE_ARTIFACT_TYPES
        protected = (
            CORE_FEATURE_TIME_COLUMNS if is_feature else CORE_LABEL_TIME_COLUMNS
        )
        try:
            schema = _snapshot_schema(snapshot, manifest.schema_id)
        except EvidenceContractError as exc:
            raise EvidenceContractError(
                "Result 缺少正式 Feature/Label 逐行表"
            ) from exc
        missing = sorted(set(protected) - set(schema.names))
        if missing:
            raise EvidenceContractError(
                f"Result 正式 Feature/Label 缺少逐行时间字段: {missing}"
            )
        key_columns = _causal_time_key_columns(schema.names, is_feature=is_feature)
        projected = tuple(dict.fromkeys((*key_columns, *protected)))
        directory = getattr(snapshot, "directory", None)
        paths = (
            tuple(Path(directory) / name for name in manifest.files)
            if directory is not None else ()
        )
        verify_causal_key_uniqueness(
            schema=pa.schema([schema.field(name) for name in key_columns]),
            batches=_snapshot_batches(snapshot, manifest.schema_id, columns=key_columns),
            parquet_paths=paths,
            budget=budget or FinancialOracleBudget(),
        )
        row_count = 0
        for batch in _snapshot_batches(
            snapshot,
            manifest.schema_id,
            columns=projected,
        ):
            if batch.num_rows == 0:
                continue
            frame = pa.Table.from_batches((batch,)).to_pandas()
            row_count += len(frame)
            if frame.loc[:, list(key_columns)].isna().any(axis=None):
                raise EvidenceContractError("Result 正式 Feature/Label 行键包含空值")
            try:
                if is_feature:
                    validate_feature_time_facts(frame)
                else:
                    validate_label_time_facts(frame)
            except CausalTimeContractError as exc:
                raise EvidenceContractError(
                    f"Result 正式 Feature/Label 逐行因果时间无效: {exc}"
                ) from exc
        if row_count <= 0:
            raise EvidenceContractError("Result 正式 Feature/Label 逐行表不能为空")


def _causal_time_key_columns(
    column_names,
    *,
    is_feature: bool,
) -> tuple[str, ...]:
    try:
        return causal_time_key_columns(column_names, is_feature=is_feature)
    except CausalTimeContractError as exc:
        raise EvidenceContractError(str(exc)) from exc


def compare_verification_results(
    left: VerifiedResultContext,
    right: VerifiedResultContext,
) -> VerificationComparison:
    require_result_consumable(left.snapshot.bundle.result_id, consumer="compare")
    require_result_consumable(right.snapshot.bundle.result_id, consumer="compare")
    reasons: set[str] = set()
    if (
        left.verification.policy_id != right.verification.policy_id
        or left.verification.policy_hash != right.verification.policy_hash
    ):
        reasons.add("comparison.claim_policy_mismatch")
    if left.verification.claim_level != right.verification.claim_level:
        reasons.add("comparison.claim_level_mismatch")
    if left.verification.claim_ceiling != right.verification.claim_ceiling:
        reasons.add("comparison.claim_ceiling_mismatch")
    left_metrics = {item.metric_ref: item for item in left.metrics}
    right_metrics = {item.metric_ref: item for item in right.metrics}
    if set(left_metrics) != set(right_metrics):
        reasons.add("comparison.metric_refs_mismatch")

    left_registry = _comparison_metric_registry(left)
    right_registry = _comparison_metric_registry(right)
    comparisons = []
    for metric_ref in sorted(set(left_metrics) & set(right_metrics)):
        left_metric = left_metrics[metric_ref]
        right_metric = right_metrics[metric_ref]
        left_definition = left_registry.require(metric_ref)
        right_definition = right_registry.require(metric_ref)
        if left_definition.definition_digest != right_definition.definition_digest:
            reasons.add("comparison.metric_definition_mismatch")
            continue
        definition = left_definition
        if (
            left_metric.unit != definition.unit
            or right_metric.unit != definition.unit
            or left_metric.unit != right_metric.unit
        ):
            reasons.add("comparison.metric_unit_mismatch")
        if (
            left_metric.sample_start != right_metric.sample_start
            or left_metric.sample_end != right_metric.sample_end
        ):
            reasons.add("comparison.metric_window_mismatch")
        if left_metric.sample_size != right_metric.sample_size:
            reasons.add("comparison.metric_sample_size_mismatch")
        if left_metric.status != right_metric.status:
            reasons.add("comparison.metric_status_mismatch")
        comparisons.append(_MetricComparison(
            metric_ref=metric_ref,
            unit=definition.unit,
            direction=definition.direction,
            sample_start=left_metric.sample_start,
            sample_end=left_metric.sample_end,
            sample_size=left_metric.sample_size,
            status=left_metric.status,
            left_value=left_metric.value,
            right_value=right_metric.value,
            delta=right_metric.value - left_metric.value,
        ))

    comparable = not reasons
    return VerificationComparison(
        comparison_scope="verified_metric_facts_only",
        scope_note=(
            "仅比较已验证指标事实；未检查 package 合同。"
            "完整研究语义比较请使用 package compare。"
        ),
        comparable=comparable,
        reason_codes=tuple(sorted(reasons)),
        same_result=left.snapshot.bundle.result_id == right.snapshot.bundle.result_id,
        same_plan=(
            left.verification.source_plan_hash
            == right.verification.source_plan_hash
        ),
        same_package=(
            left.verification.source_package_hash
            == right.verification.source_package_hash
        ),
        same_implementation=(
            left.verification.source_implementation_manifest_hash
            == right.verification.source_implementation_manifest_hash
        ),
        metric_comparisons=tuple(comparisons) if comparable else (),
    )


def _comparison_metric_registry(context: VerifiedResultContext):
    identity = context.snapshot.bundle.verification.verifier_identity
    project_definitions = (
        ()
        if identity is None
        else tuple(
            MetricDefinition.from_dict(item)
            for item in identity["metric_definitions"]
        )
    )
    return compose_metric_registry(project_definitions)


def export_verified_result(context: VerifiedResultContext, destination: str | Path) -> Path:
    require_result_consumable(
        context.snapshot.bundle.result_id,
        consumer="export",
    )
    target = Path(destination).resolve()
    if target.exists():
        raise EvidenceContractError("export-result 目标必须不存在")
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise EvidenceContractError("export-result 临时目录已存在")
    bundle = context.snapshot.bundle
    relative = Path(bundle.project_id) / bundle.run_id / bundle.result_id
    copied = temporary / relative
    try:
        copied.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(context.snapshot.directory, copied)
        if ResultStore(temporary, create=False).verify(copied) != bundle:
            raise EvidenceContractError("export-result 写后 Result 验证失败")
        os.replace(temporary, target)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target / relative


__all__ = [
    "BUILTIN_RESULT_SCHEMA_IDENTITIES", "BUILTIN_RESULT_SCHEMA_SET_IDENTITIES",
    "BUILTIN_VERIFIER_IDENTITIES",
    "VERIFICATION_RESULT_VERSION", "VerificationComparison", "VerificationReportModel",
    "VerificationResult", "VerifiedResultContext", "build_verification_report",
    "compare_verification_results", "load_verified_result_context",
    "export_verified_result", "render_verification_report", "verify_result",
    "validate_builtin_verification_semantics", "write_verification_result",
]
