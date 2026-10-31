"""列式数据平面端到端服务。"""

from __future__ import annotations

import json
import os
from contextlib import nullcontext
from pathlib import Path
from factor_contracts import FactorReadLease
from research_pipeline.catalog import CompiledCatalog, DuckDBSourceInspector
from research_pipeline.platform.canonical import canonical_json

from .admission import AdmittedQueryPlan
from .admitted_plan_codec import admitted_plan_from_dict
from .errors import SnapshotIntegrityError
from .providers import DuckDBColumnarProvider
from .revision import probe_duckdb_source, require_source_unchanged
from .snapshot_identity import build_logical_snapshot
from .snapshots import publish_parquet_snapshot
from .dataset_artifacts import build_dataset_artifact_ref
from .execution_budget import DataPlaneExecutionBudget
from .execution_estimate import ExecutionEstimate
from .factor_publication import require_factor_publication_unchanged


DATA_PLANE_PROVIDER_VERSION = "duckdb-arrow-provider-v2"
DATA_PLANE_COMPILER_VERSION = "duckdb-parameterized-sql-v2"
MINUTE_DATA_PLANE_PROVIDER_VERSION = "duckdb-arrow-provider-minute-v3"
MINUTE_DATA_PLANE_COMPILER_VERSION = "duckdb-parameterized-sql-minute-v3"


def _implementation_versions(plan: AdmittedQueryPlan) -> tuple[str, str]:
    if plan.minute_dataset_semantics_hash is not None:
        return MINUTE_DATA_PLANE_PROVIDER_VERSION, MINUTE_DATA_PLANE_COMPILER_VERSION
    return DATA_PLANE_PROVIDER_VERSION, DATA_PLANE_COMPILER_VERSION


def _write_json_atomic(path: str | Path, payload: dict[str, object]) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.write_text(canonical_json(payload), encoding="utf-8")
    os.replace(temporary, target)
    return target


def load_compiled_catalog(lock: str | Path) -> CompiledCatalog:
    path = Path(lock).resolve()
    if path.is_dir():
        return CompiledCatalog.load(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return CompiledCatalog.from_payload(payload)


def save_plan(path: str | Path, plan: AdmittedQueryPlan) -> Path:
    return _write_json_atomic(path, plan.to_dict())


def load_plan(path: str | Path) -> AdmittedQueryPlan:
    return admitted_plan_from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def _database_fingerprint(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _materialize_dataset_plan_locked(
    *,
    plan: AdmittedQueryPlan,
    data_db: str | Path,
    artifact_root: str | Path,
    execution_budget: DataPlaneExecutionBudget,
    execution_estimate: ExecutionEstimate,
    scratch_root: str | Path,
) -> dict[str, object]:
    """只发布可验证 Parquet 数据集，不为大窗口额外整体物化共享表。"""
    import duckdb

    database = Path(data_db).resolve()
    # 保留只读连接，让 schema、revision 与 Provider 共用底层数据库实例，
    # 避免每次复验都重新加载源库目录；各次观察与查询仍独立执行。
    with duckdb.connect(str(database), read_only=True):
        root = Path(artifact_root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        before_fingerprint = _database_fingerprint(database)
        if plan.factor_publication is not None:
            require_factor_publication_unchanged(
                plan.factor_publication,
                database,
                storage_table=plan.object_name,
            )
        inventory = DuckDBSourceInspector(
            database,
            source_profile=plan.source_profile,
            environment=plan.environment,
        ).observe_current_schema(plan.object_name)
        if inventory.schema_revision != plan.expected_schema_revision:
            raise SnapshotIntegrityError("materialize 前 schema 与漂移证明不一致")
        before_revision = probe_duckdb_source(
            database,
            allowed_root=database.parent,
            object_name=plan.object_name,
            source_id=plan.binding_id,
        )
        provider = DuckDBColumnarProvider(database)
        temporal_source = plan.temporal_selection.requires_consumer_binding
        stream_context = (
            provider.open_temporal_source_stream(
                plan,
                execution_budget=execution_budget,
                execution_estimate=execution_estimate,
                scratch_root=scratch_root,
            )
            if temporal_source
            else provider.open_stream(
                plan,
                execution_budget=execution_budget,
                execution_estimate=execution_estimate,
                scratch_root=scratch_root,
            )
        )
        with stream_context as stream:
            provider_version, compiler_version = _implementation_versions(plan)
            logical = build_logical_snapshot(
                plan,
                source_revisions=(before_revision,),
                output_schema=stream.schema,
                provider_version=provider_version,
                compiler_version=compiler_version,
            )

            def before_commit() -> None:
                after = probe_duckdb_source(
                    database,
                    allowed_root=database.parent,
                    object_name=plan.object_name,
                    source_id=plan.binding_id,
                )
                require_source_unchanged(before_revision, after)
                if plan.factor_publication is not None:
                    require_factor_publication_unchanged(
                        plan.factor_publication,
                        database,
                        storage_table=plan.object_name,
                    )

            snapshot_path = publish_parquet_snapshot(
                batches=stream,
                schema=stream.schema,
                plan=plan,
                logical_snapshot=logical,
                root=root / "snapshots",
                before_commit=before_commit,
                temporal_source=temporal_source,
            )
        reference = build_dataset_artifact_ref(snapshot_path, allowed_root=root)
        if plan.factor_publication is not None:
            require_factor_publication_unchanged(
                plan.factor_publication,
                database,
                storage_table=plan.object_name,
            )
        after_fingerprint = _database_fingerprint(database)
        if before_fingerprint != after_fingerprint:
            raise SnapshotIntegrityError("只读 dataset materialize 前后数据库指纹发生变化")
        return {
            "status": "pass",
            "plan_hash": logical.admitted_plan_hash,
            "dataset_id": plan.query.dataset_id,
            "reference": reference.to_dict(),
            "database_unchanged": True,
            "database_fingerprint_before": before_fingerprint,
            "database_fingerprint_after": after_fingerprint,
        }


def materialize_dataset_plan(
    *,
    plan: AdmittedQueryPlan,
    data_db: str | Path,
    artifact_root: str | Path,
    execution_budget: DataPlaneExecutionBudget,
    execution_estimate: ExecutionEstimate,
    scratch_root: str | Path,
) -> dict[str, object]:
    """因子源在一个共享租约内完成身份核对、读取、提交和最终复验。"""

    database = Path(data_db).resolve()
    lease = FactorReadLease(database) if plan.factor_publication else nullcontext()
    with lease:
        if plan.factor_publication is not None:
            require_factor_publication_unchanged(
                plan.factor_publication,
                database,
                storage_table=plan.object_name,
            )
        result = _materialize_dataset_plan_locked(
            plan=plan,
            data_db=database,
            artifact_root=artifact_root,
            execution_budget=execution_budget,
            execution_estimate=execution_estimate,
            scratch_root=scratch_root,
        )
        if plan.factor_publication is not None:
            require_factor_publication_unchanged(
                plan.factor_publication,
                database,
                storage_table=plan.object_name,
            )
        return result


__all__ = ["load_compiled_catalog", "load_plan", "materialize_dataset_plan", "reference_dataset_plan", "save_plan"]


def reference_dataset_plan(*, plan, data_db, artifact_root, execution_budget,
                           execution_estimate, scratch_root):
    """只冻结小型来源描述；实际数据由消费者分批读取。"""
    from dataclasses import asdict
    from .direct_sources import DirectSourceSchema, no_spill_budget, publish_direct_descriptor, open_projected_stream

    database = Path(data_db).resolve()
    budget, estimate = no_spill_budget(execution_budget, execution_estimate)
    lease = FactorReadLease(database) if plan.factor_publication else nullcontext()
    with lease:
        before = _database_fingerprint(database)
        inventory = DuckDBSourceInspector(database, source_profile=plan.source_profile,
                                          environment=plan.environment).observe_current_schema(plan.object_name)
        if inventory.schema_revision != plan.expected_schema_revision:
            raise SnapshotIntegrityError("直接来源 schema 与准入证明不一致")
        revision = probe_duckdb_source(database, allowed_root=database.parent,
                                      object_name=plan.object_name, source_id=plan.binding_id)
        if plan.factor_publication:
            require_factor_publication_unchanged(plan.factor_publication, database, storage_table=plan.object_name)
        temporal = plan.temporal_selection.requires_consumer_binding
        columns = plan.temporal_selection.required_scan_fields if temporal else plan.query.field_ids
        with open_projected_stream(database, plan, budget, estimate,
                                   columns=columns, filters=(), schema_only=True) as stream:
            schema = asdict(DirectSourceSchema.from_arrow_schema(stream.schema))
        after_revision = probe_duckdb_source(database, allowed_root=database.parent,
                                            object_name=plan.object_name, source_id=plan.binding_id)
        require_source_unchanged(revision, after_revision)
        if plan.factor_publication:
            require_factor_publication_unchanged(plan.factor_publication, database, storage_table=plan.object_name)
        after = _database_fingerprint(database)
        if before != after:
            raise SnapshotIntegrityError("直接来源创建前后发生变化")
        payload = {
            "contract_version": "direct-source-descriptor-v1", "source_kind": "duckdb",
            "database": str(database), "plan": plan.to_dict(),
            "admitted_plan_hash": plan.plan_hash, "temporal_source": temporal,
            "schema": schema, "execution_budget": budget.to_dict(),
            "execution_estimate": estimate.to_dict(), "source_revision": revision.to_dict(),
            "provider_version": _implementation_versions(plan)[0],
            "compiler_version": _implementation_versions(plan)[1],
        }
        reference = publish_direct_descriptor(payload, artifact_root=artifact_root)
        return {"status": "pass", "plan_hash": plan.plan_hash,
                "dataset_id": plan.query.dataset_id, "reference": reference.to_dict(),
                "database_unchanged": True, "database_fingerprint_before": before,
                "database_fingerprint_after": after}
