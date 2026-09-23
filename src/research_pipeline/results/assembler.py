"""只选择、校验并发布 Runtime 已提交工件的 ResultAssembler。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, Mapping

from research_pipeline.data_plane import (
    PARTITIONED_DATASET_REF_CONTRACT,
    DatasetArtifactRef,
    PartitionedDatasetRef,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.claim_levels import weakest_claim_level
from research_pipeline.platform.metric_contracts import MetricReachabilityProof
from research_pipeline.platform.operator_contracts import operator_dag_runtime_hash
from .artifact_reader import ExternalArtifactReader, RuntimeArtifactReference
from .contracts import (
    ResultBundle,
    ResultInputRevision,
    ResultReference,
    ResultRunSummary,
    ResultSpec,
    ResultSupportFile,
    ResultTableManifest,
    ResultVerificationClosure,
)
from .errors import ResultContractError
from .runtime_projection import OPERATOR_DAG_RUN_VERSION
from .runtime_projection import load_runtime_run_projection
from .store import ResultStore


RESULT_REFERENCE_FILE = "result-ref.json"
_BAR_TCA_SUPPORT_PATHS = (
    "simulation/result-contract/manifest.json",
    "simulation/result-contract/COMMITTED",
    "simulation/tca/manifest.json",
    "simulation/tca/COMMITTED",
    "simulation/tca/oracle-input.json",
)
_MINUTE_FINANCIAL_CONTEXT_PATH = "simulation/context.json"
_DAILY_ETF_FINANCIAL_CONTEXT_PATH = "simulation/daily-context.json"


class ResultAssembler:
    """不访问数据库、不运行研究算法，也不改变任何 claim。"""

    def __init__(self, result_store: str | Path) -> None:
        self.store = ResultStore(result_store)

    def finalize(
        self,
        *,
        run_root: str | Path,
        project_id: str,
        package_hash: str,
        plan_hash: str,
        result_spec: ResultSpec,
        catalog_hashes: Mapping[str, str],
        data_references: Mapping[str, object],
        metric_proofs: tuple[MetricReachabilityProof, ...],
        implementation_manifest_hash: str,
        verification_policy_id: str,
        validity_producer_hash: str,
        verifier_identity: Mapping[str, object] | None = None,
        formal_input_request_ids: tuple[str, ...] | None = None,
        input_claim_ceilings: Mapping[str, str] | None = None,
        phase_hook: Callable[[str], None] | None = None,
        published_hook: Callable[[ResultBundle, Path], None] | None = None,
    ) -> tuple[ResultBundle, Path, ResultReference]:
        root = Path(run_root).resolve()
        try:
            record = json.loads((root / "operator-dag-run.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ResultContractError("ResultAssembler 无法读取 Runtime 终态") from exc
        if (
            not isinstance(record, Mapping)
            or record.get("contract_version") != OPERATOR_DAG_RUN_VERSION
            or record.get("status") != "succeeded"
        ):
            raise ResultContractError("ResultAssembler 只接受 terminal succeeded run")
        outputs = record.get("outputs")
        if not isinstance(outputs, Mapping):
            raise ResultContractError("Runtime 终态缺少 outputs")
        selected_metric_tables = {
            (
                item.source_node_id,
                item.source_port,
                item.artifact_type,
                item.table_id,
                item.schema_id,
                item.path_prefix,
            )
            for item in result_spec.tables
        }
        uncovered_metrics = sorted(
            proof.metric_ref
            for proof in metric_proofs
            if (
                proof.producer_node_id,
                proof.producer_port,
                proof.artifact_type,
                proof.result_table_id,
                proof.result_schema_id,
                proof.result_path_prefix,
            ) not in selected_metric_tables
        )
        if uncovered_metrics:
            raise ResultContractError(
                f"ResultSpec 未覆盖 metric producer: {uncovered_metrics}"
            )
        input_revisions = self._input_revisions(data_references)
        external = ExternalArtifactReader(root / "external-artifacts")
        tables = []
        commits = {}
        for spec in result_spec.tables:
            node_outputs = outputs.get(spec.source_node_id)
            raw_reference = (
                node_outputs.get(spec.source_port)
                if isinstance(node_outputs, Mapping)
                else None
            )
            if not isinstance(raw_reference, dict):
                raise ResultContractError(
                    f"ResultSpec source node/port 没有 Runtime 输出: "
                    f"{spec.source_node_id}/{spec.source_port}"
                )
            reference = RuntimeArtifactReference.from_dict(raw_reference)
            if reference.name != spec.source_port or reference.artifact_type != spec.artifact_type:
                raise ResultContractError(f"ResultSpec source port/type 与 Runtime 输出不一致: {spec.table_id}")
            commit = commits.get(reference.artifact_key)
            if commit is None:
                commit = external.inspect(reference.artifact_key)
                commits[reference.artifact_key] = commit
            if commit.artifact_reference != reference:
                raise ResultContractError("Runtime output 与 ExternalArtifactCommit 身份不一致")
            prefix = f"{spec.path_prefix}/"
            source_files = {
                path: digest
                for path, digest in commit.files.items()
                if path.startswith(prefix) and path.endswith(".parquet")
            }
            if not source_files:
                raise ResultContractError(f"ResultSpec 没有选中正式 Parquet 表: {spec.table_id}")
            files = {
                f"tables/{spec.table_id}/{path.removeprefix(prefix)}": digest
                for path, digest in source_files.items()
            }
            tables.append(ResultTableManifest.build(
                spec=spec,
                artifact_key=commit.semantic_hash,
                artifact_manifest_hash=commit.manifest_hash,
                files=files,
                schema_hashes={
                    f"tables/{spec.table_id}/{path.removeprefix(prefix)}":
                    commit.schema_hashes[path]
                    for path in source_files
                },
                row_counts={
                    f"tables/{spec.table_id}/{path.removeprefix(prefix)}":
                    commit.row_counts[path]
                    for path in source_files
                },
            ))
        validity_support, validity_facts = self._validity_support_file(
            record=record,
            external=external,
            commits=commits,
        )
        data_pit = validity_facts.get("data_pit")
        if not isinstance(data_pit, Mapping):
            raise ResultContractError("validity facts 缺少 data_pit")
        runtime_request_ids = data_pit.get("consumed_request_ids")
        runtime_ceilings = data_pit.get("input_claim_ceilings")
        runtime_effective = data_pit.get("effective_claim_ceiling")
        if (
            not isinstance(runtime_request_ids, list)
            or not isinstance(runtime_ceilings, Mapping)
        ):
            raise ResultContractError("validity facts 缺少输入 claim lineage")
        selected_request_ids = tuple(
            str(item) for item in (
                runtime_request_ids
                if formal_input_request_ids is None
                else formal_input_request_ids
            )
        )
        selected_ceilings = {
            str(key): str(value)
            for key, value in (
                runtime_ceilings
                if input_claim_ceilings is None
                else input_claim_ceilings
            ).items()
        }
        effective_input_ceiling = weakest_claim_level(
            *selected_ceilings.values()
        )
        if (
            list(selected_request_ids) != runtime_request_ids
            or selected_ceilings != dict(runtime_ceilings)
            or effective_input_ceiling != runtime_effective
        ):
            raise ResultContractError(
                "Result plan claim lineage 与 Runtime validity facts 不一致"
            )
        support_files = self._support_files(
            result_spec,
            commits,
            external=external,
            validity_support=validity_support,
        )
        audit_environment = record.get("audit_environment")
        if not isinstance(audit_environment, Mapping):
            raise ResultContractError("Runtime 终态缺少 audit environment")
        execution_identity_hash = typed_canonical_hash({
            key: record.get(key)
            for key in (
                "contract_version", "project_id", "run_id", "dag_id", "dag", "root_seed",
                "fixed_clock", "mode", "audit_environment", "audit_manifest_digest",
            )
        })
        runtime_hash = operator_dag_runtime_hash(record)
        completion_metadata = record.get("completion_metadata")
        if not isinstance(completion_metadata, Mapping):
            raise ResultContractError("Runtime 终态缺少 completion metadata")
        backend_fidelity_hash = typed_canonical_hash({
            "audit_environment": dict(audit_environment),
            "audit_manifest_digest": record.get("audit_manifest_digest"),
            "execution_mode": record.get("mode"),
            "completion_metadata": dict(completion_metadata),
        })
        try:
            run_projection = load_runtime_run_projection(
                record_content=canonical_json(dict(record)).encode("utf-8"),
                event_content=(root / "events.jsonl").read_bytes(),
            )
        except (OSError, ResultContractError) as exc:
            raise ResultContractError("Result 无法形成终态运行摘要") from exc
        run_summary = ResultRunSummary(
            project_id=run_projection.project_id,
            run_id=run_projection.run_id,
            parent_run_id=run_projection.parent_run_id,
            dag_id=run_projection.dag_id,
            status=run_projection.status,
            mode=run_projection.mode,
            fixed_clock=run_projection.fixed_clock,
            event_chain_head=run_projection.event_chain_head,
            node_statuses=run_projection.node_statuses,
        )
        verification = ResultVerificationClosure(
            policy_id=verification_policy_id,
            validity_artifact_key=validity_support.artifact_key,
            validity_source_path=validity_support.source_path,
            validity_content_hash=validity_support.content_hash,
            validity_producer_hash=validity_producer_hash,
            run=run_summary,
            verifier_identity=verifier_identity,
        )
        bundle = ResultBundle.build(
            project_id=project_id,
            run_id=str(record.get("run_id")),
            package_hash=package_hash,
            plan_hash=plan_hash,
            catalog_hashes=dict(sorted(catalog_hashes.items())),
            input_revisions=input_revisions,
            formal_input_request_ids=selected_request_ids,
            input_claim_ceilings=selected_ceilings,
            effective_input_claim_ceiling=effective_input_ceiling,
            execution_identity_hash=execution_identity_hash,
            runtime_hash=runtime_hash,
            implementation_manifest_hash=implementation_manifest_hash,
            backend_fidelity_hash=backend_fidelity_hash,
            result_spec=result_spec,
            metric_proofs=tuple(sorted(metric_proofs, key=lambda item: item.metric_ref)),
            tables=tuple(tables),
            verification=verification,
            support_files=support_files,
        )
        published_reported = False

        def report_published(directory: Path) -> None:
            nonlocal published_reported
            if published_hook is not None and not published_reported:
                published_hook(bundle, directory)
                published_reported = True

        def track_publish(phase: str) -> None:
            if phase == "renamed":
                report_published(self.store.result_directory(bundle))
            if phase_hook is not None:
                phase_hook(phase)

        destination = self.store.publish(
            bundle,
            run_root=root,
            phase_hook=track_publish,
        )
        # 幂等 finalize 命中既有同一 Result 时不会再次触发 renamed。
        report_published(destination)
        reference = ResultReference.build(bundle)
        self._write_reference(root, reference)
        return bundle, destination, reference

    @staticmethod
    def _input_revisions(data_references: Mapping[str, object]) -> tuple[ResultInputRevision, ...]:
        if not data_references:
            raise ResultContractError("ResultBundle 必须绑定实际 input manifest/revision")
        revisions = []
        for request_id, payload in sorted(data_references.items()):
            if not isinstance(payload, Mapping):
                raise ResultContractError("data reference 必须是映射")
            try:
                if payload.get("contract_version") == PARTITIONED_DATASET_REF_CONTRACT:
                    partitioned = PartitionedDatasetRef.from_dict(payload)
                    source_revision_hash = partitioned.lineage.get("source_revision_hash")
                    if not isinstance(source_revision_hash, str):
                        raise ResultContractError("分钟分区引用缺少来源 revision")
                    revisions.append(ResultInputRevision(
                        request_id=request_id,
                        physical_snapshot_id=partitioned.reference_id,
                        manifest_hash=partitioned.reference_id,
                        schema_hash=partitioned.schema_hash,
                        source_revision_hash=source_revision_hash,
                    ))
                    continue
                reference = DatasetArtifactRef.from_dict(payload)
            except Exception as exc:
                raise ResultContractError("data reference 合同无效") from exc
            revisions.append(ResultInputRevision(
                request_id=request_id,
                physical_snapshot_id=reference.physical_snapshot_id,
                manifest_hash=reference.manifest_hash,
                schema_hash=reference.schema_hash,
                source_revision_hash=reference.source_revision_hash,
            ))
        return tuple(revisions)

    @staticmethod
    def _validity_support_file(
        *,
        record: Mapping[str, object],
        external: ExternalArtifactReader,
        commits: dict[str, object],
    ) -> tuple[ResultSupportFile, Mapping[str, object]]:
        """从 Runtime 终态选择唯一 validity facts，不依赖 run-root 约定路径。"""

        outputs = record.get("outputs")
        if not isinstance(outputs, Mapping):
            raise ResultContractError("Runtime 终态缺少 validity 输出")
        matches: list[RuntimeArtifactReference] = []
        for node_outputs in outputs.values():
            if not isinstance(node_outputs, Mapping):
                continue
            for raw_reference in node_outputs.values():
                if not isinstance(raw_reference, Mapping):
                    continue
                reference = RuntimeArtifactReference.from_dict(raw_reference)
                if reference.artifact_type == "research.validity-facts.v1":
                    matches.append(reference)
        if len(matches) != 1:
            raise ResultContractError("Result 必须绑定唯一 validity facts 输出")
        reference = matches[0]
        commit = commits.get(reference.artifact_key)
        if commit is None:
            commit = external.inspect(reference.artifact_key)
            commits[reference.artifact_key] = commit
        if commit.artifact_reference != reference or "result.json" not in commit.files:
            raise ResultContractError("validity facts 输出未完整提交")
        try:
            payload = json.loads(
                external.read_bytes(commit, "result.json").decode("utf-8")
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultContractError("validity facts result.json 无法读取") from exc
        required_fact_sections = {
            "data_pit",
            "label_split",
            "search_holdout",
            "statistics",
            "financial_tradability",
        }
        if (
            not isinstance(payload, Mapping)
            or payload.get("contract_version") != "research-validity-facts-v1"
            or not required_fact_sections.issubset(payload)
        ):
            raise ResultContractError(
                "Result 只接受完整 research validity facts，不能封存 gates 摘要"
            )
        return (
            ResultSupportFile(
                artifact_key=commit.semantic_hash,
                artifact_type="research.validity-facts.v1",
                source_path="result.json",
                relative_path=f"support/{commit.semantic_hash}/result.json",
                content_hash=commit.files["result.json"],
            ),
            payload,
        )

    @staticmethod
    def _support_files(
        result_spec: ResultSpec,
        commits: Mapping[str, object],
        *,
        external: ExternalArtifactReader,
        validity_support: ResultSupportFile,
    ) -> tuple[ResultSupportFile, ...]:
        """封装 validity facts，以及按 ResultSpec 条件触发的金融控制文件。"""

        from .contracts import BAR_TCA_SCHEMA_IDS

        declared = {item.schema_id for item in result_spec.tables}
        if not declared & set(BAR_TCA_SCHEMA_IDS.values()):
            return (validity_support,)
        selected = [validity_support]
        financial_commits: dict[str, object] = {}
        for source_path in _BAR_TCA_SUPPORT_PATHS:
            matches = tuple(commit for commit in commits.values() if source_path in commit.files)
            if len(matches) != 1:
                raise ResultContractError(f"Bar TCA Result 缺少唯一控制文件: {source_path}")
            commit = matches[0]
            financial_commits[source_path] = commit
            selected.append(ResultSupportFile(
                artifact_key=commit.semantic_hash,
                artifact_type="research.financial-control.v1",
                source_path=source_path,
                relative_path=f"support/{commit.semantic_hash}/{source_path}",
                content_hash=commit.files[source_path],
            ))
        try:
            simulation_manifest = json.loads(
                external.read_bytes(
                    financial_commits["simulation/result-contract/manifest.json"],
                    "simulation/result-contract/manifest.json",
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultContractError("SimulationResult manifest 无法读取") from exc
        semantics = simulation_manifest.get("semantics")
        requires_daily_etf_context = (
            isinstance(semantics, Mapping)
            and semantics.get("asset_class") == "cn_etf"
            and semantics.get("frequency") == "daily"
        )
        context_matches = tuple(
            commit for commit in commits.values()
            if _MINUTE_FINANCIAL_CONTEXT_PATH in commit.files
        )
        if len(context_matches) > 1:
            raise ResultContractError("分钟金融上下文必须来自唯一仿真工件")
        if context_matches:
            commit = context_matches[0]
            selected.append(ResultSupportFile(
                artifact_key=commit.semantic_hash,
                artifact_type="research.financial-control.v1",
                source_path=_MINUTE_FINANCIAL_CONTEXT_PATH,
                relative_path=(
                    f"support/{commit.semantic_hash}/"
                    f"{_MINUTE_FINANCIAL_CONTEXT_PATH}"
                ),
                content_hash=commit.files[_MINUTE_FINANCIAL_CONTEXT_PATH],
            ))
        daily_context_matches = tuple(
            commit for commit in commits.values()
            if _DAILY_ETF_FINANCIAL_CONTEXT_PATH in commit.files
        )
        if len(daily_context_matches) > 1:
            raise ResultContractError("ETF 日频金融上下文必须来自唯一仿真工件")
        if requires_daily_etf_context and len(daily_context_matches) != 1:
            raise ResultContractError("ETF 日频 Result 缺少受控市场规则上下文")
        if daily_context_matches:
            commit = daily_context_matches[0]
            selected.append(ResultSupportFile(
                artifact_key=commit.semantic_hash,
                artifact_type="research.financial-control.v1",
                source_path=_DAILY_ETF_FINANCIAL_CONTEXT_PATH,
                relative_path=(
                    f"support/{commit.semantic_hash}/"
                    f"{_DAILY_ETF_FINANCIAL_CONTEXT_PATH}"
                ),
                content_hash=commit.files[_DAILY_ETF_FINANCIAL_CONTEXT_PATH],
            ))
        return tuple(sorted(
            selected, key=lambda item: (item.artifact_key, item.source_path)
        ))

    @staticmethod
    def _write_reference(root: Path, reference: ResultReference) -> None:
        target = root / RESULT_REFERENCE_FILE
        document = canonical_json(reference.to_dict())
        if target.exists():
            if target.read_text(encoding="utf-8") != document:
                raise ResultContractError("run result reference 与既有文件冲突")
            return
        temporary = target.with_suffix(".tmp")
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(document)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)


__all__ = ["RESULT_REFERENCE_FILE", "ResultAssembler"]
