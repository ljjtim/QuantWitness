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
from research_pipeline.platform.metric_contracts import MetricDefinition, MetricReachabilityProof
from research_pipeline.platform.operator_contracts import operator_dag_runtime_hash
from .artifact_reader import ExternalArtifactReader, RuntimeArtifactReference
from .contracts import (
    QLIB_MODEL_INVENTORY_SCHEMA_ID,
    ResultBundle,
    ResultInputRevision,
    ResultReference,
    ResultRunSummary,
    ResultSpec,
    ResultSupportFile,
    ResultTableManifest,
)
from .errors import ResultContractError
from .runtime_projection import OPERATOR_DAG_RUN_VERSION
from .runtime_projection import load_runtime_run_projection
from .store import ResultStore


RESULT_REFERENCE_FILE = "result-ref.json"
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
        claim_level: str,
        formal_input_request_ids: tuple[str, ...],
        input_claim_ceilings: Mapping[str, str],
        metric_definitions: tuple[MetricDefinition, ...] = (),
        research_cost_assumption: Mapping[str, object] | None = None,
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
        selected_request_ids = tuple(formal_input_request_ids)
        selected_ceilings = dict(input_claim_ceilings)
        effective_input_ceiling = weakest_claim_level(*selected_ceilings.values())
        support_files = self._support_files(
            result_spec, commits, external=external, tables=tuple(tables),
            include_model_ledger=any(
                table.artifact_type == "research.model-locked-holdout.v2" for table in tables
            ),
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
            node_statuses=run_projection.node_statuses,
            limitations=tuple(completion_metadata.get("limitations", ())),
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
            claim_level=claim_level,
            execution_identity_hash=execution_identity_hash,
            runtime_hash=runtime_hash,
            implementation_manifest_hash=implementation_manifest_hash,
            backend_fidelity_hash=backend_fidelity_hash,
            result_spec=result_spec,
            metric_proofs=tuple(sorted(metric_proofs, key=lambda item: item.metric_ref)),
            tables=tuple(tables),
            run=run_summary,
            support_files=support_files,
            metric_definitions=tuple(sorted(metric_definitions, key=lambda item: item.metric_ref)),
            research_cost_assumption=research_cost_assumption,
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
            if reference.contract_version == "direct-dataset-ref-v1":
                from .contracts import ResultSourceRevision
                revisions.append(ResultSourceRevision(
                    request_id=request_id, source_identity=reference.source_identity,
                    descriptor_hash=reference.descriptor_hash, schema_hash=reference.schema_hash,
                    source_revision_hash=reference.source_revision_hash))
                continue
            revisions.append(ResultInputRevision(
                request_id=request_id,
                physical_snapshot_id=reference.physical_snapshot_id,
                manifest_hash=reference.manifest_hash,
                schema_hash=reference.schema_hash,
                source_revision_hash=reference.source_revision_hash,
            ))
        return tuple(revisions)

    @staticmethod
    def _support_files(
        result_spec: ResultSpec,
        commits: Mapping[str, object],
        *,
        external: ExternalArtifactReader,
        tables: tuple[ResultTableManifest, ...] = (),
        include_model_ledger: bool = False,
    ) -> tuple[ResultSupportFile, ...]:
        """按正式表声明封存正式报告所需控制与模型状态。"""

        declared = {item.schema_id for item in result_spec.tables}
        selected = []
        selected.extend(ResultAssembler._model_support_files(
            tables, commits, external=external,
        ))
        if include_model_ledger:
            holdout_keys = {
                item.artifact_key for item in tables
                if item.artifact_type == "research.model-locked-holdout.v2"
            }
            if len(holdout_keys) != 1:
                raise ResultContractError("模型有效性要求唯一正式 holdout 工件")
            commit = commits[next(iter(holdout_keys))]
            if "holdout-ledger/plan.json" not in commit.files:
                raise ResultContractError("模型 Result 缺少完整 holdout 账本")
            plan = json.loads(external.read_bytes(commit, "holdout-ledger/plan.json"))
            ledger_names = ["plan", "prepared", "opened", "terminal"]
            if "research_series" in plan.get("freeze_payload", {}):
                ledger_names.append("series")
            for name in ledger_names:
                source_path = f"holdout-ledger/{name}.json"
                if source_path not in commit.files:
                    raise ResultContractError("模型 Result 缺少完整 holdout 账本")
                selected.append(ResultSupportFile(
                    artifact_key=commit.semantic_hash,
                    artifact_type="research.model-locked-holdout.v2",
                    source_path=source_path,
                    relative_path=f"support/{commit.semantic_hash}/{source_path}",
                    content_hash=commit.files[source_path],
                ))
        from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE, LEAN_SCHEMA_IDS, LEAN_SUPPORT_PATHS

        if declared & set(LEAN_SCHEMA_IDS.values()):
            lean_tables = [table for table in tables if table.schema_id in LEAN_SCHEMA_IDS.values()]
            if {table.schema_id for table in lean_tables} != set(LEAN_SCHEMA_IDS.values()):
                raise ResultContractError("LEAN Result 缺少完整执行表")
            keys = {table.artifact_key for table in lean_tables}
            if len(keys) != 1 or any(table.artifact_type != LEAN_ARTIFACT_TYPE for table in lean_tables):
                raise ResultContractError("LEAN Result 执行表必须来自同一已提交工件")
            lean_commit = commits[next(iter(keys))]
            for source_path in LEAN_SUPPORT_PATHS:
                if source_path not in lean_commit.files:
                    raise ResultContractError(f"LEAN Result 缺少支持文件: {source_path}")
                selected.append(ResultSupportFile(
                    artifact_key=lean_commit.semantic_hash,
                    artifact_type=LEAN_ARTIFACT_TYPE,
                    source_path=source_path,
                    relative_path=f"support/{lean_commit.semantic_hash}/{source_path}",
                    content_hash=lean_commit.files[source_path],
                ))
        return tuple(sorted(
            selected, key=lambda item: (item.artifact_key, item.source_path)
        ))

    @staticmethod
    def _model_support_files(
        tables: tuple[ResultTableManifest, ...],
        commits: Mapping[str, object],
        *,
        external: ExternalArtifactReader,
    ) -> tuple[ResultSupportFile, ...]:
        """只封存 ResultSpec 选中清单里成功模型明确引用的文件。"""

        selected: dict[tuple[str, str], ResultSupportFile] = {}
        for table in tables:
            if table.schema_id != QLIB_MODEL_INVENTORY_SCHEMA_ID:
                continue
            commit = commits[table.artifact_key]
            source_prefix = f"{table.path_prefix}/"
            for source_path in sorted(commit.files):
                if not source_path.startswith(source_prefix) or not source_path.endswith(".parquet"):
                    continue
                inventory = external.read_parquet(commit, source_path)
                required = {"status", "config_path", "model_path"}
                if not required.issubset(inventory.column_names):
                    raise ResultContractError("Qlib 模型清单缺少状态或文件路径")
                for row in inventory.to_pylist():
                    if row["status"] != "fitted":
                        continue
                    config_path = row["config_path"]
                    if not isinstance(config_path, str) or config_path not in commit.files:
                        raise ResultContractError(f"Qlib 模型引用未提交文件: {config_path}")
                    try:
                        config = json.loads(external.read_bytes(commit, config_path))
                    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
                        raise ResultContractError("Qlib 模型配置无法读取") from exc
                    if not isinstance(config, dict) or config.get("model_path") != row["model_path"]:
                        raise ResultContractError("Qlib 模型清单与配置的模型路径不一致")
                    processor_files = config.get("processor_files")
                    if not isinstance(processor_files, dict) or set(processor_files) != {"infer", "learn"}:
                        raise ResultContractError("Qlib 模型配置缺少 Processor 文件清单")
                    paths = [config_path, row["model_path"]]
                    if config.get("schema") not in {"research.qlib-model-bundle.v1", "research.qlib-sequence-model-bundle.v2"}:
                        raise ResultContractError("Qlib 模型配置 schema 不受支持")
                    if config["schema"] == "research.qlib-sequence-model-bundle.v2":
                        sequence = config.get("sequence")
                        sequence_files = sequence.get("files") if isinstance(sequence, dict) else None
                        if (not isinstance(sequence, dict)
                                or sequence.get("schema") != "research.qlib-sequence-input.v2"
                                or not isinstance(sequence_files, dict)
                                or set(sequence_files) != {"context", "targets", "members"}):
                            raise ResultContractError("Qlib 序列模型配置缺少窗口文件清单")
                        paths.append(config.get("weights_path"))
                        paths.extend(sequence_files[name] for name in ("context", "targets", "members"))
                    if config.get("candidate", {}).get("model", {}).get("class") == "GeneratedModel":
                        generated = config.get("generated")
                        if not isinstance(generated, dict):
                            raise ResultContractError("生成模型配置缺少源码与权重清单")
                        paths.extend((generated.get("source_path"), generated.get("weights_path")))
                    for role in ("infer", "learn"):
                        if not isinstance(processor_files[role], list):
                            raise ResultContractError("Qlib Processor 文件清单必须为列表")
                        paths.extend(processor_files[role])
                    for path in paths:
                        if not isinstance(path, str) or path not in commit.files:
                            raise ResultContractError(f"Qlib 模型引用未提交文件: {path}")
                    for path in paths:
                        key = (commit.semantic_hash, path)
                        selected[key] = ResultSupportFile(
                            artifact_key=commit.semantic_hash,
                            artifact_type=table.artifact_type,
                            source_path=path,
                            relative_path=f"support/{commit.semantic_hash}/{path}",
                            content_hash=commit.files[path],
                        )
        return tuple(selected[key] for key in sorted(selected))

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
