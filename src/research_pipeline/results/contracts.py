"""`research-result-v2` 的纯数据合同与身份规则。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from types import MappingProxyType
from typing import Mapping, Sequence

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.claim_levels import CLAIM_LEVELS, weakest_claim_level
from research_pipeline.platform.metric_contracts import MetricReachabilityProof
from .errors import ResultContractError


RESULT_SPEC_VERSION = "research-result-spec-v1"
RESULT_BUNDLE_VERSION = "research-result-v2"
RESULT_TABLE_MANIFEST_VERSION = "research-result-table-manifest-v2"
RESULT_SUPPORT_FILE_VERSION = "research-result-support-file-v2"
RESULT_RUN_SUMMARY_VERSION = "research-result-run-summary-v1"
RESULT_VERIFICATION_CLOSURE_VERSION = "research-result-verification-closure-v1"
RESULT_INPUT_REVISION_VERSION = "research-result-input-revision-v1"
RESULT_REF_VERSION = "research-result-ref-v1"

_SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TABLE_ROLES = frozenset({"primary", "diagnostic", "metrics", "portfolio", "orders", "trades"})

CANONICAL_SIMULATION_SCHEMA_IDS = MappingProxyType({
    name: f"research.simulation.{name}.v1"
    for name in ("orders", "fills", "positions", "cash", "costs", "valuations")
})
BAR_TCA_SCHEMA_IDS = MappingProxyType({
    name: f"research.bar-tca.{name}.v1"
    for name in ("orders", "fills", "daily", "research")
})
MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS = MappingProxyType({
    "execution_bars": "research.minute-financial-context.execution-bars.v1",
    "decision_benchmarks": (
        "research.minute-financial-context.decision-benchmarks.v1"
    ),
    "execution_observations": (
        "research.minute-financial-context.execution-observations.v1"
    ),
    "settlement_events": "research.minute-financial-context.settlement-events.v1",
})


def _require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise ResultContractError(f"{field} 必须是安全稳定 ID")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ResultContractError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_exact(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(payload) != expected:
        raise ResultContractError(
            f"{field} schema 不匹配；缺失={sorted(expected - set(payload))}，"
            f"未知={sorted(set(payload) - expected)}"
        )


def _safe_prefix(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ResultContractError("result table path_prefix 必须是安全 POSIX 相对路径")
    parts = value.split("/")
    if any(not part or part in {".", ".."} or ":" in part for part in parts):
        raise ResultContractError("result table path_prefix 必须是安全 POSIX 相对路径")
    return "/".join(parts)


def _digest_mapping(value: Mapping[str, str], field: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise ResultContractError(f"{field} 必须是非空摘要映射")
    normalized: dict[str, str] = {}
    for key, digest in value.items():
        if not isinstance(key, str) or not key or "\\" in key:
            raise ResultContractError(f"{field} 含非法路径")
        parts = key.split("/")
        if any(not part or part in {".", ".."} or ":" in part for part in parts):
            raise ResultContractError(f"{field} 含非法路径")
        _require_hash(digest, f"{field}.{key}")
        normalized[key] = digest
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class ResultTableSpec:
    table_id: str
    role: str
    source_node_id: str
    source_port: str
    artifact_type: str
    schema_id: str
    path_prefix: str

    def __post_init__(self) -> None:
        for field in ("table_id", "source_node_id", "source_port", "artifact_type", "schema_id"):
            _require_id(getattr(self, field), f"result table {field}")
        if self.role not in _TABLE_ROLES:
            raise ResultContractError("result table role 不受支持")
        object.__setattr__(self, "path_prefix", _safe_prefix(self.path_prefix))

    def to_dict(self) -> dict[str, str]:
        return {
            "table_id": self.table_id,
            "role": self.role,
            "source_node_id": self.source_node_id,
            "source_port": self.source_port,
            "artifact_type": self.artifact_type,
            "schema_id": self.schema_id,
            "path_prefix": self.path_prefix,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultTableSpec":
        expected = {
            "table_id", "role", "source_node_id", "source_port",
            "artifact_type", "schema_id", "path_prefix",
        }
        _require_exact(payload, expected, "ResultTableSpec")
        if any(not isinstance(payload[field], str) for field in expected):
            raise ResultContractError("ResultTableSpec 字段必须是字符串")
        return cls(*(str(payload[field]) for field in (
            "table_id", "role", "source_node_id", "source_port",
            "artifact_type", "schema_id", "path_prefix",
        )))


@dataclass(frozen=True)
class ResultSpec:
    tables: tuple[ResultTableSpec, ...]
    result_spec_hash: str
    contract_version: str = RESULT_SPEC_VERSION

    def __post_init__(self) -> None:
        table_ids = tuple(item.table_id for item in self.tables)
        if not self.tables or table_ids != tuple(sorted(table_ids)) or len(table_ids) != len(set(table_ids)):
            raise ResultContractError("ResultSpec table_id 必须非空、唯一并规范排序")
        if sum(item.role == "primary" for item in self.tables) != 1:
            raise ResultContractError("ResultSpec 必须恰好有一个 primary table")
        identities = tuple(
            (
                item.source_node_id,
                item.source_port,
                item.artifact_type,
                item.path_prefix,
            )
            for item in self.tables
        )
        if len(identities) != len(set(identities)):
            raise ResultContractError(
                "ResultSpec 不允许重复选择同一 source/port/artifact/path_prefix"
            )
        schema_ids = tuple(item.schema_id for item in self.tables)
        if len(schema_ids) != len(set(schema_ids)):
            raise ResultContractError("ResultSpec schema_id 必须唯一")
        declared = set(schema_ids)
        tca_ids = set(BAR_TCA_SCHEMA_IDS.values())
        if declared & tca_ids:
            required = tca_ids | set(CANONICAL_SIMULATION_SCHEMA_IDS.values())
            if not required <= declared:
                raise ResultContractError("Bar TCA 公共结果必须同时包含规范六表和四张 TCA 表")
        minute_context_ids = set(MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS.values())
        if declared & minute_context_ids and not minute_context_ids <= declared:
            raise ResultContractError("分钟金融上下文必须完整包含三张列式事实表")
        if self.contract_version != RESULT_SPEC_VERSION:
            raise ResultContractError("ResultSpec 版本不受支持")
        if self.result_spec_hash != typed_canonical_hash(self.payload()):
            raise ResultContractError("ResultSpec hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "tables": [item.to_dict() for item in self.tables],
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "result_spec_hash": self.result_spec_hash}

    @classmethod
    def build(cls, tables: Sequence[ResultTableSpec]) -> "ResultSpec":
        ordered = tuple(sorted(tables, key=lambda item: item.table_id))
        payload = {
            "tables": [item.to_dict() for item in ordered],
            "contract_version": RESULT_SPEC_VERSION,
        }
        return cls(ordered, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultSpec":
        _require_exact(payload, {"tables", "result_spec_hash", "contract_version"}, "ResultSpec")
        tables = payload["tables"]
        if not isinstance(tables, (list, tuple)) or any(not isinstance(item, Mapping) for item in tables):
            raise ResultContractError("ResultSpec tables 必须是映射列表")
        return cls(
            tuple(ResultTableSpec.from_dict(item) for item in tables),
            str(payload["result_spec_hash"]),
            str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class ResultInputRevision:
    request_id: str
    physical_snapshot_id: str
    manifest_hash: str
    schema_hash: str
    source_revision_hash: str
    contract_version: str = RESULT_INPUT_REVISION_VERSION

    def __post_init__(self) -> None:
        _require_id(self.request_id, "input revision request_id")
        for field in ("physical_snapshot_id", "manifest_hash", "schema_hash", "source_revision_hash"):
            _require_hash(getattr(self, field), f"input revision {field}")
        if self.contract_version != RESULT_INPUT_REVISION_VERSION:
            raise ResultContractError("ResultInputRevision 版本不受支持")

    def to_dict(self) -> dict[str, str]:
        return {
            "request_id": self.request_id,
            "physical_snapshot_id": self.physical_snapshot_id,
            "manifest_hash": self.manifest_hash,
            "schema_hash": self.schema_hash,
            "source_revision_hash": self.source_revision_hash,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultInputRevision":
        expected = {
            "request_id", "physical_snapshot_id", "manifest_hash", "schema_hash",
            "source_revision_hash", "contract_version",
        }
        _require_exact(payload, expected, "ResultInputRevision")
        return cls(*(str(payload[field]) for field in (
            "request_id", "physical_snapshot_id", "manifest_hash", "schema_hash",
            "source_revision_hash", "contract_version",
        )))


@dataclass(frozen=True)
class ResultTableManifest:
    table_id: str
    role: str
    source_node_id: str
    source_port: str
    artifact_type: str
    schema_id: str
    path_prefix: str
    artifact_key: str
    artifact_manifest_hash: str
    files: Mapping[str, str]
    schema_hashes: Mapping[str, str]
    row_counts: Mapping[str, int]
    table_manifest_hash: str
    contract_version: str = RESULT_TABLE_MANIFEST_VERSION

    def __post_init__(self) -> None:
        ResultTableSpec(
            self.table_id, self.role, self.source_node_id, self.source_port,
            self.artifact_type, self.schema_id, self.path_prefix,
        )
        _require_hash(self.artifact_key, "result table artifact_key")
        _require_hash(self.artifact_manifest_hash, "result table artifact_manifest_hash")
        object.__setattr__(self, "files", _digest_mapping(self.files, "result table files"))
        object.__setattr__(self, "schema_hashes", _digest_mapping(self.schema_hashes, "result table schema_hashes"))
        rows = {
            key: value for key, value in self.row_counts.items()
            if isinstance(key, str) and type(value) is int and value >= 0
        }
        if (
            len(rows) != len(self.row_counts)
            or set(self.files) != set(self.schema_hashes)
            or set(self.files) != set(rows)
            or any(not path.endswith(".parquet") for path in self.files)
            or any(
                not path.startswith(f"tables/{self.table_id}/")
                for path in self.files
            )
        ):
            raise ResultContractError("正式结果表必须由闭合的 Parquet schema/row_count manifest 描述")
        object.__setattr__(self, "row_counts", MappingProxyType(dict(sorted(rows.items()))))
        if self.contract_version != RESULT_TABLE_MANIFEST_VERSION:
            raise ResultContractError("ResultTableManifest 版本不受支持")
        if self.table_manifest_hash != typed_canonical_hash(self.payload()):
            raise ResultContractError("ResultTableManifest hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            **ResultTableSpec(
                self.table_id, self.role, self.source_node_id, self.source_port,
                self.artifact_type, self.schema_id, self.path_prefix,
            ).to_dict(),
            "artifact_key": self.artifact_key,
            "artifact_manifest_hash": self.artifact_manifest_hash,
            "files": dict(self.files),
            "schema_hashes": dict(self.schema_hashes),
            "row_counts": dict(self.row_counts),
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "table_manifest_hash": self.table_manifest_hash}

    @classmethod
    def build(
        cls,
        *,
        spec: ResultTableSpec,
        artifact_key: str,
        artifact_manifest_hash: str,
        files: Mapping[str, str],
        schema_hashes: Mapping[str, str],
        row_counts: Mapping[str, int],
    ) -> "ResultTableManifest":
        values = {
            **spec.to_dict(),
            "artifact_key": artifact_key,
            "artifact_manifest_hash": artifact_manifest_hash,
            "files": dict(sorted(files.items())),
            "schema_hashes": dict(sorted(schema_hashes.items())),
            "row_counts": dict(sorted(row_counts.items())),
            "contract_version": RESULT_TABLE_MANIFEST_VERSION,
        }
        return cls(
            spec.table_id, spec.role, spec.source_node_id, spec.source_port,
            spec.artifact_type, spec.schema_id, spec.path_prefix,
            artifact_key, artifact_manifest_hash, files, schema_hashes, row_counts,
            typed_canonical_hash(values),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultTableManifest":
        expected = {
            "table_id", "role", "source_node_id", "source_port", "artifact_type",
            "schema_id", "path_prefix", "artifact_key", "artifact_manifest_hash",
            "files", "schema_hashes", "row_counts", "table_manifest_hash", "contract_version",
        }
        _require_exact(payload, expected, "ResultTableManifest")
        if any(not isinstance(payload[field], Mapping) for field in ("files", "schema_hashes", "row_counts")):
            raise ResultContractError("ResultTableManifest 映射字段无效")
        return cls(
            *(str(payload[field]) for field in (
                "table_id", "role", "source_node_id", "source_port", "artifact_type",
                "schema_id", "path_prefix", "artifact_key", "artifact_manifest_hash",
            )),
            dict(payload["files"]),
            dict(payload["schema_hashes"]),
            dict(payload["row_counts"]),
            str(payload["table_manifest_hash"]),
            str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class ResultSupportFile:
    """随最终表进入 Result 的小型验证材料。"""

    artifact_key: str
    artifact_type: str
    source_path: str
    relative_path: str
    content_hash: str
    contract_version: str = RESULT_SUPPORT_FILE_VERSION

    def __post_init__(self) -> None:
        _require_hash(self.artifact_key, "result support artifact_key")
        _require_hash(self.content_hash, "result support content_hash")
        _require_id(self.artifact_type, "result support artifact_type")
        source_path = _safe_prefix(self.source_path)
        relative_path = _safe_prefix(self.relative_path)
        expected = f"support/{self.artifact_key}/{source_path}"
        if relative_path != expected:
            raise ResultContractError("Result support file 必须使用 Result 内部相对路径")
        if source_path.endswith(".parquet"):
            raise ResultContractError("Result support file 不能代替正式 Parquet 表")
        if self.contract_version != RESULT_SUPPORT_FILE_VERSION:
            raise ResultContractError("ResultSupportFile 版本不受支持")

    def to_dict(self) -> dict[str, str]:
        return {
            "artifact_key": self.artifact_key,
            "artifact_type": self.artifact_type,
            "source_path": self.source_path,
            "relative_path": self.relative_path,
            "content_hash": self.content_hash,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultSupportFile":
        expected = {
            "artifact_key", "artifact_type", "source_path", "relative_path", "content_hash",
            "contract_version",
        }
        _require_exact(payload, expected, "ResultSupportFile")
        return cls(*(str(payload[field]) for field in (
            "artifact_key", "artifact_type", "source_path", "relative_path", "content_hash",
            "contract_version",
        )))


@dataclass(frozen=True)
class ResultRunSummary:
    """进入 Result 身份的终态运行摘要，普通消费者不再依赖 run-root。"""

    project_id: str
    run_id: str
    parent_run_id: str | None
    dag_id: str
    status: str
    mode: str
    fixed_clock: str
    event_chain_head: str
    node_statuses: Mapping[str, str]
    contract_version: str = RESULT_RUN_SUMMARY_VERSION

    def __post_init__(self) -> None:
        _require_id(self.project_id, "result run project_id")
        for field in ("run_id", "dag_id", "event_chain_head"):
            _require_hash(getattr(self, field), f"result run {field}")
        if self.parent_run_id is not None:
            _require_hash(self.parent_run_id, "result run parent_run_id")
        if self.status != "succeeded":
            raise ResultContractError("Result 只接受 succeeded 运行摘要")
        if self.mode not in {
            "deterministic_serial", "bounded_parallel", "partitioned_batch",
        }:
            raise ResultContractError("Result 运行模式无效")
        try:
            clock = datetime.fromisoformat(self.fixed_clock)
        except ValueError as exc:
            raise ResultContractError("Result fixed_clock 不是 ISO 时间") from exc
        if clock.utcoffset() is None:
            raise ResultContractError("Result fixed_clock 必须带 UTC offset")
        statuses = dict(sorted(self.node_statuses.items()))
        if (
            not statuses
            or any(not isinstance(key, str) or not key for key in statuses)
            or set(statuses.values()) != {"succeeded"}
        ):
            raise ResultContractError("Result 节点终态必须完整 succeeded")
        object.__setattr__(self, "node_statuses", MappingProxyType(statuses))
        if self.contract_version != RESULT_RUN_SUMMARY_VERSION:
            raise ResultContractError("ResultRunSummary 版本不受支持")

    def to_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "run_id": self.run_id,
            "parent_run_id": self.parent_run_id,
            "dag_id": self.dag_id,
            "status": self.status,
            "mode": self.mode,
            "fixed_clock": self.fixed_clock,
            "event_chain_head": self.event_chain_head,
            "node_statuses": dict(self.node_statuses),
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultRunSummary":
        expected = {
            "project_id", "run_id", "parent_run_id", "dag_id", "status", "mode",
            "fixed_clock", "event_chain_head", "node_statuses", "contract_version",
        }
        _require_exact(payload, expected, "ResultRunSummary")
        if not isinstance(payload["node_statuses"], Mapping):
            raise ResultContractError("ResultRunSummary node_statuses 必须是映射")
        parent = payload["parent_run_id"]
        return cls(
            project_id=str(payload["project_id"]),
            run_id=str(payload["run_id"]),
            parent_run_id=None if parent is None else str(parent),
            dag_id=str(payload["dag_id"]),
            status=str(payload["status"]),
            mode=str(payload["mode"]),
            fixed_clock=str(payload["fixed_clock"]),
            event_chain_head=str(payload["event_chain_head"]),
            node_statuses={str(key): str(value) for key, value in payload["node_statuses"].items()},
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class ResultVerificationClosure:
    """Verifier 直接从 ResultStore 读取的最小当前验证闭包。"""

    policy_id: str
    validity_artifact_key: str
    validity_source_path: str
    validity_content_hash: str
    validity_producer_hash: str
    run: ResultRunSummary
    verifier_identity: Mapping[str, object] | None = None
    contract_version: str = RESULT_VERIFICATION_CLOSURE_VERSION

    def __post_init__(self) -> None:
        _require_id(self.policy_id, "result verification policy_id")
        for field in (
            "validity_artifact_key", "validity_content_hash", "validity_producer_hash",
        ):
            _require_hash(getattr(self, field), f"result verification {field}")
        object.__setattr__(
            self, "validity_source_path", _safe_prefix(self.validity_source_path)
        )
        if self.contract_version != RESULT_VERIFICATION_CLOSURE_VERSION:
            raise ResultContractError("ResultVerificationClosure 版本不受支持")
        if self.verifier_identity is not None:
            if not isinstance(self.verifier_identity, Mapping):
                raise ResultContractError("ResultVerifier identity 必须是映射")
            required = {
                "project_id", "verifier_id", "verifier_version", "bundle_hash",
                "implementation_hash", "authorized_schema_ids",
                "authorized_support_artifact_types", "metric_definitions", "abi_version",
            }
            if set(self.verifier_identity) != required:
                raise ResultContractError("ResultVerifier identity schema 无效")
            object.__setattr__(self, "verifier_identity", dict(self.verifier_identity))

    def to_dict(self) -> dict[str, object]:
        payload = {
            "policy_id": self.policy_id,
            "validity_artifact_key": self.validity_artifact_key,
            "validity_source_path": self.validity_source_path,
            "validity_content_hash": self.validity_content_hash,
            "validity_producer_hash": self.validity_producer_hash,
            "run": self.run.to_dict(),
            "contract_version": self.contract_version,
        }
        if self.verifier_identity is not None:
            payload["verifier_identity"] = dict(self.verifier_identity)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultVerificationClosure":
        expected = {
            "policy_id", "validity_artifact_key", "validity_source_path",
            "validity_content_hash", "validity_producer_hash", "run",
            "contract_version",
        }
        if "verifier_identity" in payload:
            expected.add("verifier_identity")
        _require_exact(payload, expected, "ResultVerificationClosure")
        if not isinstance(payload["run"], Mapping):
            raise ResultContractError("ResultVerificationClosure run 必须是映射")
        return cls(
            policy_id=str(payload["policy_id"]),
            validity_artifact_key=str(payload["validity_artifact_key"]),
            validity_source_path=str(payload["validity_source_path"]),
            validity_content_hash=str(payload["validity_content_hash"]),
            validity_producer_hash=str(payload["validity_producer_hash"]),
            run=ResultRunSummary.from_dict(payload["run"]),
            verifier_identity=(
                None
                if "verifier_identity" not in payload
                else dict(payload["verifier_identity"])
            ),
            contract_version=str(payload["contract_version"]),
        )

@dataclass(frozen=True)
class ResultBundle:
    project_id: str
    run_id: str
    package_hash: str
    plan_hash: str
    catalog_hashes: Mapping[str, str]
    input_revisions: tuple[ResultInputRevision, ...]
    formal_input_request_ids: tuple[str, ...]
    input_claim_ceilings: Mapping[str, str]
    effective_input_claim_ceiling: str
    execution_identity_hash: str
    runtime_hash: str
    implementation_manifest_hash: str
    backend_fidelity_hash: str
    result_spec: ResultSpec
    metric_proofs: tuple[MetricReachabilityProof, ...]
    tables: tuple[ResultTableManifest, ...]
    verification: ResultVerificationClosure
    result_id: str
    support_files: tuple[ResultSupportFile, ...] = ()
    status: str = "finalized"
    contract_version: str = RESULT_BUNDLE_VERSION

    def __post_init__(self) -> None:
        _require_id(self.project_id, "result project_id")
        for field in (
            "run_id", "package_hash", "plan_hash", "execution_identity_hash", "runtime_hash",
            "implementation_manifest_hash", "backend_fidelity_hash", "result_id",
        ):
            _require_hash(getattr(self, field), f"result {field}")
        object.__setattr__(self, "catalog_hashes", _digest_mapping(self.catalog_hashes, "catalog_hashes"))
        request_ids = tuple(item.request_id for item in self.input_revisions)
        if not request_ids or request_ids != tuple(sorted(request_ids)) or len(request_ids) != len(set(request_ids)):
            raise ResultContractError("ResultBundle input revisions 必须非空、唯一并规范排序")
        if (
            not self.formal_input_request_ids
            or self.formal_input_request_ids
            != tuple(sorted(self.formal_input_request_ids))
            or len(self.formal_input_request_ids)
            != len(set(self.formal_input_request_ids))
            or not set(self.formal_input_request_ids) <= set(request_ids)
        ):
            raise ResultContractError("ResultBundle 正式输入请求集合无效")
        if (
            not isinstance(self.input_claim_ceilings, Mapping)
            or set(self.input_claim_ceilings) != set(self.formal_input_request_ids)
            or any(value not in CLAIM_LEVELS for value in self.input_claim_ceilings.values())
            or self.effective_input_claim_ceiling not in CLAIM_LEVELS
        ):
            raise ResultContractError("ResultBundle 输入 claim ceiling 无效")

        if self.effective_input_claim_ceiling != weakest_claim_level(
            *(str(value) for value in self.input_claim_ceilings.values())
        ):
            raise ResultContractError("ResultBundle 有效输入 claim ceiling 不一致")
        object.__setattr__(
            self,
            "input_claim_ceilings",
            MappingProxyType(dict(sorted(self.input_claim_ceilings.items()))),
        )
        proof_refs = tuple(item.metric_ref for item in self.metric_proofs)
        if not proof_refs or proof_refs != tuple(sorted(proof_refs)) or len(proof_refs) != len(set(proof_refs)):
            raise ResultContractError("ResultBundle metric proofs 必须非空、唯一并规范排序")
        selected_metric_tables = {
            (
                item.source_node_id,
                item.source_port,
                item.artifact_type,
                item.table_id,
                item.schema_id,
                item.path_prefix,
            )
            for item in self.result_spec.tables
        }
        if any(
            (
                proof.producer_node_id,
                proof.producer_port,
                proof.artifact_type,
                proof.result_table_id,
                proof.result_schema_id,
                proof.result_path_prefix,
            )
            not in selected_metric_tables
            for proof in self.metric_proofs
        ):
            raise ResultContractError("ResultBundle metric proof 未绑定实际指标表")
        table_ids = tuple(item.table_id for item in self.tables)
        if table_ids != tuple(item.table_id for item in self.result_spec.tables):
            raise ResultContractError("ResultBundle tables 与 ResultSpec 不闭合")
        for spec, manifest in zip(self.result_spec.tables, self.tables, strict=True):
            if spec.to_dict() != {
                key: manifest.to_dict()[key]
                for key in spec.to_dict()
            }:
                raise ResultContractError("ResultBundle table manifest 与 ResultSpec 漂移")
        support_identities = tuple(
            (item.artifact_key, item.source_path) for item in self.support_files
        )
        if (
            support_identities != tuple(sorted(support_identities))
            or len(support_identities) != len(set(support_identities))
        ):
            raise ResultContractError("ResultBundle support files 必须唯一并规范排序")
        validity_support = tuple(
            item
            for item in self.support_files
            if (
                item.artifact_key == self.verification.validity_artifact_key
                and item.source_path == self.verification.validity_source_path
                and item.content_hash == self.verification.validity_content_hash
                and item.artifact_type == "research.validity-facts.v1"
            )
        )
        if len(validity_support) != 1:
            raise ResultContractError("ResultBundle 缺少唯一 validity facts 验证材料")
        if (
            self.verification.run.project_id != self.project_id
            or self.verification.run.run_id != self.run_id
        ):
            raise ResultContractError("ResultBundle 运行摘要与结果身份不一致")
        if self.status != "finalized" or self.contract_version != RESULT_BUNDLE_VERSION:
            raise ResultContractError("ResultBundle 状态或版本无效")
        if self.result_id != typed_canonical_hash(self.payload()):
            raise ResultContractError("ResultBundle result_id 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "run_id": self.run_id,
            "package_hash": self.package_hash,
            "plan_hash": self.plan_hash,
            "catalog_hashes": dict(self.catalog_hashes),
            "input_revisions": [item.to_dict() for item in self.input_revisions],
            "formal_input_request_ids": list(self.formal_input_request_ids),
            "input_claim_ceilings": dict(self.input_claim_ceilings),
            "effective_input_claim_ceiling": self.effective_input_claim_ceiling,
            "execution_identity_hash": self.execution_identity_hash,
            "runtime_hash": self.runtime_hash,
            "implementation_manifest_hash": self.implementation_manifest_hash,
            "backend_fidelity_hash": self.backend_fidelity_hash,
            "result_spec": self.result_spec.to_dict(),
            "metric_proofs": [item.to_dict() for item in self.metric_proofs],
            "tables": [item.to_dict() for item in self.tables],
            "verification": self.verification.to_dict(),
            "support_files": [item.to_dict() for item in self.support_files],
            "status": self.status,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def build(cls, **kwargs: object) -> "ResultBundle":
        input_revisions = tuple(kwargs["input_revisions"])
        formal_input_request_ids = tuple(
            kwargs.get(
                "formal_input_request_ids",
                tuple(item.request_id for item in input_revisions),
            )
        )
        input_claim_ceilings = dict(
            kwargs.get(
                "input_claim_ceilings",
                {
                    request_id: "tradable_simulation"
                    for request_id in formal_input_request_ids
                },
            )
        )
        effective_input_claim_ceiling = str(
            kwargs.get(
                "effective_input_claim_ceiling",
                "tradable_simulation",
            )
        )
        payload = {
            "project_id": kwargs["project_id"],
            "run_id": kwargs["run_id"],
            "package_hash": kwargs["package_hash"],
            "plan_hash": kwargs["plan_hash"],
            "catalog_hashes": dict(sorted(dict(kwargs["catalog_hashes"]).items())),
            "input_revisions": [item.to_dict() for item in input_revisions],
            "formal_input_request_ids": list(formal_input_request_ids),
            "input_claim_ceilings": dict(sorted(input_claim_ceilings.items())),
            "effective_input_claim_ceiling": effective_input_claim_ceiling,
            "execution_identity_hash": kwargs["execution_identity_hash"],
            "runtime_hash": kwargs["runtime_hash"],
            "implementation_manifest_hash": kwargs["implementation_manifest_hash"],
            "backend_fidelity_hash": kwargs["backend_fidelity_hash"],
            "result_spec": kwargs["result_spec"].to_dict(),
            "metric_proofs": [item.to_dict() for item in kwargs["metric_proofs"]],
            "tables": [item.to_dict() for item in kwargs["tables"]],
            "verification": kwargs["verification"].to_dict(),
            "support_files": [item.to_dict() for item in kwargs.get("support_files", ())],
            "status": "finalized",
            "contract_version": RESULT_BUNDLE_VERSION,
        }
        values = dict(kwargs)
        values["input_revisions"] = input_revisions
        values["formal_input_request_ids"] = formal_input_request_ids
        values["input_claim_ceilings"] = input_claim_ceilings
        values["effective_input_claim_ceiling"] = effective_input_claim_ceiling
        values["support_files"] = tuple(kwargs.get("support_files", ()))
        return cls(**values, result_id=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultBundle":
        expected = {
            "project_id", "run_id", "package_hash", "plan_hash", "catalog_hashes",
            "input_revisions", "execution_identity_hash", "runtime_hash",
            "formal_input_request_ids", "input_claim_ceilings",
            "effective_input_claim_ceiling",
            "implementation_manifest_hash", "backend_fidelity_hash", "result_spec",
            "metric_proofs", "tables", "verification", "support_files", "result_id", "status",
            "contract_version",
        }
        _require_exact(payload, expected, "ResultBundle")
        for field in (
            "catalog_hashes",
            "input_claim_ceilings",
            "result_spec",
            "verification",
        ):
            if not isinstance(payload[field], Mapping):
                raise ResultContractError(f"ResultBundle {field} 必须是映射")
        for field in (
            "input_revisions",
            "metric_proofs",
            "tables",
            "support_files",
        ):
            if not isinstance(payload[field], list) or any(not isinstance(item, Mapping) for item in payload[field]):
                raise ResultContractError(f"ResultBundle {field} 必须是映射列表")
        formal_input_request_ids = payload["formal_input_request_ids"]
        if (
            not isinstance(formal_input_request_ids, list)
            or any(not isinstance(item, str) or not item for item in formal_input_request_ids)
        ):
            raise ResultContractError("ResultBundle formal_input_request_ids 必须是非空字符串列表")
        return cls(
            project_id=str(payload["project_id"]),
            run_id=str(payload["run_id"]),
            package_hash=str(payload["package_hash"]),
            plan_hash=str(payload["plan_hash"]),
            catalog_hashes=dict(payload["catalog_hashes"]),
            input_revisions=tuple(ResultInputRevision.from_dict(item) for item in payload["input_revisions"]),
            formal_input_request_ids=tuple(formal_input_request_ids),
            input_claim_ceilings={
                str(key): str(value)
                for key, value in payload["input_claim_ceilings"].items()
            },
            effective_input_claim_ceiling=str(payload["effective_input_claim_ceiling"]),
            execution_identity_hash=str(payload["execution_identity_hash"]),
            runtime_hash=str(payload["runtime_hash"]),
            implementation_manifest_hash=str(payload["implementation_manifest_hash"]),
            backend_fidelity_hash=str(payload["backend_fidelity_hash"]),
            result_spec=ResultSpec.from_dict(payload["result_spec"]),
            metric_proofs=tuple(MetricReachabilityProof.from_dict(item) for item in payload["metric_proofs"]),
            tables=tuple(ResultTableManifest.from_dict(item) for item in payload["tables"]),
            verification=ResultVerificationClosure.from_dict(payload["verification"]),
            support_files=tuple(
                ResultSupportFile.from_dict(item) for item in payload["support_files"]
            ),
            result_id=str(payload["result_id"]),
            status=str(payload["status"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class ResultReference:
    project_id: str
    run_id: str
    result_id: str
    reference_hash: str
    contract_version: str = RESULT_REF_VERSION

    def __post_init__(self) -> None:
        _require_id(self.project_id, "result reference project_id")
        _require_hash(self.run_id, "result reference run_id")
        _require_hash(self.result_id, "result reference result_id")
        if self.contract_version != RESULT_REF_VERSION:
            raise ResultContractError("ResultReference 版本不受支持")
        if self.reference_hash != typed_canonical_hash(self.payload()):
            raise ResultContractError("ResultReference hash 不一致")

    def payload(self) -> dict[str, str]:
        return {
            "project_id": self.project_id,
            "run_id": self.run_id,
            "result_id": self.result_id,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, str]:
        return {**self.payload(), "reference_hash": self.reference_hash}

    @classmethod
    def build(cls, bundle: ResultBundle) -> "ResultReference":
        payload = {
            "project_id": bundle.project_id,
            "run_id": bundle.run_id,
            "result_id": bundle.result_id,
            "contract_version": RESULT_REF_VERSION,
        }
        return cls(bundle.project_id, bundle.run_id, bundle.result_id, typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ResultReference":
        expected = {"project_id", "run_id", "result_id", "reference_hash", "contract_version"}
        _require_exact(payload, expected, "ResultReference")
        return cls(*(str(payload[field]) for field in (
            "project_id", "run_id", "result_id", "reference_hash", "contract_version",
        )))


__all__ = [
    "BAR_TCA_SCHEMA_IDS", "CANONICAL_SIMULATION_SCHEMA_IDS",
    "MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS", "RESULT_BUNDLE_VERSION",
    "RESULT_INPUT_REVISION_VERSION", "RESULT_REF_VERSION",
    "RESULT_SPEC_VERSION", "RESULT_SUPPORT_FILE_VERSION", "RESULT_TABLE_MANIFEST_VERSION", "ResultBundle",
    "ResultInputRevision", "ResultReference", "ResultSpec", "ResultTableManifest",
    "ResultSupportFile", "ResultTableSpec",
]
