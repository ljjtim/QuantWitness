"""只选择、校验并发布 Runtime 已提交工件的 ResultAssembler。"""

from __future__ import annotations

from datetime import datetime
import hashlib
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
from research_pipeline.runtime.errors import RuntimeContractError, RuntimeIntegrityError, RuntimeStateError
from research_pipeline.runtime.state import RuntimeProjection
from research_pipeline.runtime.store import EventStore, OPERATOR_DAG_RUN_VERSION
from research_pipeline.runtime.contracts import ArtifactRef
from .artifact_reader import ExternalArtifactReader
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
        runtime_record: Mapping[str, object] | None = None,
        runtime_projection: RuntimeProjection | None = None,
    ) -> tuple[ResultBundle, Path, ResultReference]:
        root = Path(run_root).resolve()
        record = runtime_record
        if record is None:
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
            try:
                reference = ArtifactRef.from_dict(raw_reference)
            except RuntimeContractError as exc:
                raise ResultContractError("Runtime ArtifactRef schema 无效") from exc
            if reference.name != spec.source_port or reference.artifact_type != spec.artifact_type:
                raise ResultContractError(f"ResultSpec source port/type 与 Runtime 输出不一致: {spec.table_id}")
            commit = commits.get(reference.artifact_key)
            if commit is None:
                commit = external.inspect(reference.artifact_key)
                commits[reference.artifact_key] = commit
            if commit.artifact_ref != reference:
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
        run_summary = self._run_summary(
            root, record, runtime_projection,
            limitations=tuple(completion_metadata.get("limitations", ())),
        )
        control_files, control_payloads = self._model_provenance_controls(
            root, record, result_spec, tuple(tables),
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
            control_files=control_files,
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

        execution_cost = self._execution_cost(root, record, completion_metadata)
        destination = self.store.publish(
            bundle,
            run_root=root,
            phase_hook=track_publish,
            execution_cost=execution_cost,
            control_payloads=control_payloads,
        )
        # 幂等 finalize 命中既有同一 Result 时不会再次触发 renamed。
        report_published(destination)
        reference = ResultReference.build(bundle)
        self._write_reference(root, reference)
        return bundle, destination, reference

    @staticmethod
    def _execution_cost(
        root: Path,
        record: Mapping[str, object],
        completion_metadata: Mapping[str, object],
    ) -> dict[str, object] | None:
        """汇总运行阶段耗时与节点成本观测事实；不参与 result_id 哈希。"""
        phase_timings = None
        timing_path = root / "run-timing.json"
        if timing_path.is_file():
            try:
                raw_timing = json.loads(timing_path.read_text(encoding="utf-8"))
                if isinstance(raw_timing, dict):
                    phase_timings = raw_timing
            except (OSError, json.JSONDecodeError):
                phase_timings = None
        nodes_summary: dict[str, dict[str, object]] = {}
        events_path = root / "events.jsonl"
        if events_path.is_file():
            try:
                events = EventStore(root).read_events()
            except (OSError, RuntimeIntegrityError, RuntimeStateError):
                events = ()
            by_node: dict[str, list] = {}
            for event in events:
                if event.node_id:
                    by_node.setdefault(event.node_id, []).append(event)
            for node_id, node_events in sorted(by_node.items()):
                reused = any(
                    e.kind == "diagnostic"
                    and e.payload.get("diagnostic_type") == "cross_run_checkpoint_reused"
                    for e in node_events
                )
                end_event = next(
                    (
                        e for e in reversed(node_events)
                        if e.kind == "node_status_changed" and e.payload.get("status") == "succeeded"
                    ),
                    None,
                )
                start_event = next(
                    (
                        e for e in reversed(node_events)
                        if e.kind == "attempt_status_changed"
                        and e.payload.get("status") == "pending"
                        and (end_event is None or e.seq <= end_event.seq)
                    ),
                    None,
                )
                duration = None
                if start_event is not None and end_event is not None:
                    try:
                        started = datetime.fromisoformat(start_event.occurred_at)
                        ended = datetime.fromisoformat(end_event.occurred_at)
                        duration = max(0.0, round((ended - started).total_seconds(), 3))
                    except ValueError:
                        duration = None
                nodes_summary[node_id] = {
                    "reused": reused,
                    "duration_seconds": duration,
                }
        imported_cost = completion_metadata.get("source_training_cost")
        if phase_timings is None and not nodes_summary and imported_cost is None:
            return None
        payload: dict[str, object] = {
            "contract_version": "research-execution-cost-v1",
            "run_id": record.get("run_id"),
            "mode": record.get("mode"),
        }
        if phase_timings is not None:
            payload["phase_timings"] = phase_timings
        if nodes_summary:
            payload["nodes"] = nodes_summary
        if imported_cost is not None:
            payload["imported_training_cost"] = imported_cost
        return payload

    @staticmethod
    def _run_summary(
        root: Path,
        record: Mapping[str, object],
        projection: RuntimeProjection | None,
        *,
        limitations: tuple,
    ) -> ResultRunSummary:
        """运行内直接消费写者快照，独立组装时从正式日志验链。"""
        if projection is None:
            try:
                projection = EventStore(root).replay()
            except (OSError, RuntimeIntegrityError, RuntimeStateError) as exc:
                raise ResultContractError("Result 无法形成终态运行摘要") from exc
        identity_fields = ("project_id", "run_id", "dag_id", "mode", "fixed_clock")
        dag = record.get("dag")
        nodes = dag.get("nodes") if isinstance(dag, Mapping) else None
        parent_run_id = record.get("parent_run_id")
        if (
            any(not isinstance(record.get(key), str) or not record[key] for key in identity_fields)
            or parent_run_id is not None and not isinstance(parent_run_id, str)
            or not isinstance(nodes, list) or not nodes
            or any(not isinstance(node, Mapping) or not isinstance(node.get("node_id"), str)
                   for node in nodes)
            or typed_canonical_hash(dag) != record["dag_id"]
        ):
            raise ResultContractError("Result 无法形成终态运行摘要，运行身份或 DAG 无效")
        node_ids = {node["node_id"] for node in nodes}
        chain_head = record.get("event_chain_head")
        if (
            projection.run_status != "succeeded"
            or projection.run_id != record["run_id"]
            or len(node_ids) != len(nodes)
            or set(projection.node_statuses) != node_ids
            or set(projection.node_statuses.values()) != {"succeeded"}
            or chain_head is not None and chain_head != projection.chain_head
        ):
            raise ResultContractError("Result 无法形成终态运行摘要，事件链与终态不一致")
        return ResultRunSummary(
            project_id=record["project_id"],
            run_id=record["run_id"],
            parent_run_id=parent_run_id,
            dag_id=record["dag_id"],
            status=projection.run_status,
            mode=record["mode"],
            fixed_clock=record["fixed_clock"],
            node_statuses=projection.node_statuses,
            limitations=limitations,
        )

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
            native_support = set(LEAN_SUPPORT_PATHS) | {
                path for path in lean_commit.files
                if path == "lean/native-support.json" or path.startswith((
                    "lean/native-output/", "lean/storage/", "lean/strategy-source/"))
            }
            for source_path in sorted(native_support):
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
    def _model_provenance_controls(
        root: Path,
        record: Mapping[str, object],
        result_spec: ResultSpec,
        tables: tuple[ResultTableManifest, ...],
    ) -> tuple[dict[str, str], dict[str, bytes]]:
        if QLIB_MODEL_INVENTORY_SCHEMA_ID not in {item.schema_id for item in result_spec.tables}:
            return {}, {}
        semantics_path = root / "research.semantics.json"
        identity_path = root / "research.identity.json"
        if not (semantics_path.is_file() and identity_path.is_file()):
            return {}, {}
        try:
            semantics = json.loads(semantics_path.read_text(encoding="utf-8"))
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultContractError("模型 Result 来源语义或身份无法读取") from exc
        dag = record.get("dag")
        raw_nodes = dag.get("nodes", ()) if isinstance(dag, Mapping) else ()
        dag_nodes = [
            {
                "node_id": node.get("node_id"),
                "implementation_id": node.get("implementation_id"),
                "output_types": list(node.get("output_types", ())),
            }
            for node in raw_nodes
            if isinstance(node, Mapping)
        ]
        fit_tables = [
            {
                "table_id": table.table_id,
                "source_node_id": table.source_node_id,
                "artifact_key": table.artifact_key,
                "schema_id": table.schema_id,
                "path_prefix": table.path_prefix,
            }
            for table in tables
            if table.artifact_type == "research.model-fits.v2"
        ]
        split_tables = [
            {
                "table_id": table.table_id,
                "source_node_id": table.source_node_id,
                "artifact_key": table.artifact_key,
                "schema_id": table.schema_id,
                "path_prefix": table.path_prefix,
            }
            for table in tables
            if table.artifact_type == "research.model-split-manifest.v2"
        ]
        payload = {
            "schema": "research.result-model-provenance.v1",
            "source_run_id": str(record.get("run_id")),
            "root_seed": record.get("root_seed"),
            "fixed_clock": record.get("fixed_clock"),
            "audit_manifest_digest": record.get("audit_manifest_digest"),
            "research_semantics": semantics,
            "research_identity": identity,
            "dag_nodes": dag_nodes,
            "fit_tables": fit_tables,
            "split_tables": split_tables,
        }
        raw_bytes = canonical_json(payload).encode("utf-8")
        digest = hashlib.sha256(raw_bytes).hexdigest()
        return {"model-provenance.json": digest}, {"model-provenance.json": raw_bytes}

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
            if table.artifact_type in {"research.model-split-manifest.v2", "research.model-fits.v2"} or table.schema_id == QLIB_MODEL_INVENTORY_SCHEMA_ID:
                commit = commits[table.artifact_key]
                if "artifact-metadata.json" in commit.files:
                    key = (commit.semantic_hash, "artifact-metadata.json")
                    selected[key] = ResultSupportFile(
                        artifact_key=commit.semantic_hash,
                        artifact_type=table.artifact_type,
                        source_path="artifact-metadata.json",
                        relative_path=f"support/{commit.semantic_hash}/artifact-metadata.json",
                        content_hash=commit.files["artifact-metadata.json"],
                    )
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
                    upstream_sources = config.get("upstream_sources", [])
                    if not isinstance(upstream_sources, list):
                        raise ResultContractError("模型上游源码清单必须为列表")
                    paths.extend(upstream_sources)
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
                    if config.get("candidate", {}).get("model", {}).get("class") == "ProjectQlibModel":
                        project_model = config.get("project_model")
                        bundle_path = project_model.get("bundle_path") if isinstance(project_model, dict) else None
                        if not isinstance(bundle_path, str) or not bundle_path:
                            raise ResultContractError("项目模型配置缺少源码bundle路径")
                        prefix = bundle_path.rstrip("/") + "/"
                        source_files = [name for name in commit.files if name.startswith(prefix)]
                        for required_file in ("manifest.json", "dependency-lock.json", "COMMITTED"):
                            if prefix + required_file not in source_files:
                                raise ResultContractError(f"项目模型源码闭包缺少文件: {required_file}")
                        if not any(name.startswith(prefix + "sources/") and name.endswith(".py") for name in source_files):
                            raise ResultContractError("项目模型源码闭包没有Python源码")
                        paths.extend(source_files)
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
