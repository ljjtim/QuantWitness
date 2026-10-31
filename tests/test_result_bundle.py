"""正式 Result 的原子发布、列式读取和指标合同。"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import os
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricDefinition, MetricReachabilityProof, build_mainline_metric_registry
from research_pipeline.platform.operator_contracts import operator_dag_runtime_hash
from research_pipeline.results import ResultAssembler, ResultContractError, ResultSnapshot, ResultSpec, ResultStore, ResultTableSpec, metrics_from_snapshot
from research_pipeline.runtime import EventStore
from research_pipeline.runtime.external_artifact import ExternalArtifactStore
import research_pipeline.results.store as result_store_module

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64


def _write_succeeded_events(
    run_root: Path,
    *,
    run_id: str,
    node_ids: tuple[str, ...],
) -> str:
    store = EventStore(run_root)
    for index, status in enumerate(("created", "planned", "running"), 1):
        store.append(
            run_id,
            "run_status_changed",
            {"status": status},
            command_id=f"run-{index}",
        )
    for node_id in node_ids:
        for index, status in enumerate(("pending", "ready", "running", "succeeded"), 1):
            store.append(
                run_id,
                "node_status_changed",
                {"status": status},
                command_id=f"{node_id}-{index}",
                node_id=node_id,
            )
    return store.append(
        run_id,
        "run_status_changed",
        {"status": "succeeded"},
        command_id="run-succeeded",
    ).event_hash



def _proof(definition: MetricDefinition | None = None) -> MetricReachabilityProof:
    definition = definition or build_mainline_metric_registry().require(
        "data.row_count@1.0.0"
    )
    metric_ref = definition.metric_ref
    payload = {
        "metric_ref": metric_ref,
        "definition_digest": definition.definition_digest,
        "producer_node_id": "statistics",
        "producer_port": "statistics",
        "artifact_type": "data.columnar-bundle.v1",
        "result_table_id": "primary_statistics",
        "result_schema_id": "data.columnar-bundle.metrics.v1",
        "result_path_prefix": "statistics",
        "contract_version": "research-metric-reachability-v2",
    }
    return MetricReachabilityProof(**payload, proof_digest=typed_canonical_hash(payload))



def _spec() -> ResultSpec:
    return ResultSpec.build((ResultTableSpec(
        table_id="primary_statistics",
        role="primary",
        source_node_id="statistics",
        source_port="statistics",
        artifact_type="data.columnar-bundle.v1",
        schema_id="data.columnar-bundle.metrics.v1",
        path_prefix="statistics",
    ),))



def _data_reference() -> dict[str, object]:
    return {
        "relative_path": "objects/a",
        "physical_snapshot_id": HASH_A,
        "manifest_hash": HASH_B,
        "schema_hash": HASH_C,
        "source_revision_hash": HASH_D,
        "partitions": ["date=2026-01-01/part-0.parquet"],
        "contract_version": "dataset-artifact-ref-v1",
    }



def _runtime_fixture(
    tmp_path: Path,
    *,
    metric_ref: str = "data.row_count@1.0.0",
    analysis_table: pa.Table | None = None,
) -> tuple[Path, Path, Path]:
    run_root = tmp_path / "run"
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    (staging / "statistics").mkdir()
    pq.write_table(
        pa.table({
            "metric_ref": [metric_ref],
            "value": [1.25],
            "unit": ["rows" if metric_ref == "data.row_count@1.0.0" else "decimal_return"],
            "sample_start": ["2026-01-01"],
            "sample_end": ["2026-06-30"],
            "sample_size": pa.array([120], type=pa.int64()),
            "status": ["computed"],
        }),
        staging / "statistics" / "metrics.parquet",
    )
    if analysis_table is not None:
        (staging / "analysis_series").mkdir()
        pq.write_table(analysis_table, staging / "analysis_series/part-00000.parquet")
    (staging / "result.json").write_text("{}", encoding="utf-8")
    commit = external.commit(
        staging,
        artifact_name="statistics",
        artifact_type="data.columnar-bundle.v1",
    )
    node_ids = ("statistics",)
    dag = {
        "contract_version": "test",
        "nodes": [{"node_id": node_id} for node_id in node_ids],
        "edges": [],
    }
    event_chain_head = _write_succeeded_events(
        run_root,
        run_id=HASH_A,
        node_ids=node_ids,
    )
    record = {
        "contract_version": "research-runtime-operator-dag-run-v3",
        "status": "succeeded",
        "project_id": "research_package_test",
        "run_id": HASH_A,
        "dag_id": typed_canonical_hash(dag),
        "dag": dag,
        "root_seed": 7,
        "fixed_clock": "2026-07-25T00:00:00+08:00",
        "mode": "deterministic_serial",
        "audit_environment": {"python": "test", "backend": "pyarrow"},
        "audit_manifest_digest": HASH_C,
        "reused_nodes": [],
        "outputs": {
            "statistics": {"statistics": commit.artifact_ref.to_dict()},
        },
        "completion_metadata": {
            "artifact_hashes": {},
            "proof_hashes": {},
            "counts": {},
            "backend_id": None,
            "fidelity": None,
            "limitations": [],
            "contract_version": "research-runtime-completion-metadata-v1",
        },
        "event_chain_head": event_chain_head,
    }
    (run_root / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8",
    )
    return run_root, tmp_path / "results", external.objects_root / commit.semantic_hash



def _dual_table_runtime_fixture(tmp_path: Path) -> tuple[Path, Path]:
    run_root = tmp_path / "run"
    external = ExternalArtifactStore(run_root / "external-artifacts")
    outputs = {}
    for node_id, port, artifact_type, prefix, table in (
        (
            "statistics",
            "statistics",
            "data.columnar-bundle.v1",
            "statistics",
            pa.table({"metric": ["alpha"], "value": [1.25]}),
        ),
        (
            "diagnostics",
            "diagnostics",
            "research.diagnostics.v1",
            "diagnostics",
            pa.table({"reason": ["sample_count"], "value": [32]}),
        ),
    ):
        staging = external.prepare()
        (staging / prefix).mkdir()
        pq.write_table(table, staging / prefix / "part-0.parquet")
        commit = external.commit(
            staging,
            artifact_name=port,
            artifact_type=artifact_type,
        )
        outputs[node_id] = {port: commit.artifact_ref.to_dict()}
    node_ids = tuple(outputs)
    dag = {
        "contract_version": "test",
        "nodes": [{"node_id": node_id} for node_id in node_ids],
        "edges": [],
    }
    event_chain_head = _write_succeeded_events(
        run_root,
        run_id=HASH_A,
        node_ids=node_ids,
    )
    record = {
        "contract_version": "research-runtime-operator-dag-run-v3",
        "status": "succeeded",
        "project_id": "research_package_test",
        "run_id": HASH_A,
        "dag_id": typed_canonical_hash(dag),
        "dag": dag,
        "root_seed": 7,
        "fixed_clock": "2026-07-25T00:00:00+08:00",
        "mode": "deterministic_serial",
        "audit_environment": {"python": "test", "backend": "pyarrow"},
        "audit_manifest_digest": HASH_C,
        "reused_nodes": [],
        "outputs": outputs,
        "completion_metadata": {
            "artifact_hashes": {},
            "proof_hashes": {},
            "counts": {},
            "backend_id": None,
            "fidelity": None,
            "limitations": [],
            "contract_version": "research-runtime-completion-metadata-v1",
        },
        "event_chain_head": event_chain_head,
    }
    (run_root / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8",
    )
    return run_root, tmp_path / "results"



def _finalize(
    tmp_path: Path, *, phase_hook=None, package_hash=HASH_B, plan_hash=HASH_C,
    result_spec=None, metric_proofs=None,
):
    run_root, result_root, object_root = _runtime_fixture(tmp_path)
    bundle, directory, reference = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=package_hash,
        plan_hash=plan_hash,
        result_spec=_spec() if result_spec is None else result_spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),) if metric_proofs is None else metric_proofs,
        implementation_manifest_hash=HASH_A,
        claim_level="research_observation",
        formal_input_request_ids=("prices",),
        input_claim_ceilings={"prices": "research_observation"},
        phase_hook=phase_hook,
    )
    return run_root, result_root, object_root, bundle, directory, reference



def test_result_identity_is_stable_across_equivalent_event_chains(
    tmp_path: Path,
) -> None:
    first_run, first_result_root, _ = _runtime_fixture(tmp_path / "first")
    second_run, second_result_root, _ = _runtime_fixture(tmp_path / "second")
    first_record = json.loads(
        (first_run / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    second_record = json.loads(
        (second_run / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    assert first_record["event_chain_head"] != second_record["event_chain_head"]
    assert first_record["outputs"] == second_record["outputs"]
    assert operator_dag_runtime_hash(first_record) == operator_dag_runtime_hash(
        second_record
    )

    finalize_kwargs = {
        "project_id": "research_package_test",
        "package_hash": HASH_B,
        "plan_hash": HASH_C,
        "result_spec": _spec(),
        "catalog_hashes": {"prices": HASH_D},
        "data_references": {"prices": _data_reference()},
        "metric_proofs": (_proof(),),
        "implementation_manifest_hash": HASH_A,
        "claim_level": "research_observation",
        "formal_input_request_ids": ("prices",),
        "input_claim_ceilings": {"prices": "research_observation"},
    }
    first_bundle, first_directory, _ = ResultAssembler(first_result_root).finalize(
        run_root=first_run,
        **finalize_kwargs,
    )
    second_bundle, second_directory, _ = ResultAssembler(second_result_root).finalize(
        run_root=second_run,
        **finalize_kwargs,
    )

    assert first_bundle == second_bundle
    assert first_bundle.result_id == second_bundle.result_id
    assert (first_directory / "result.json").read_bytes() == (
        second_directory / "result.json"
    ).read_bytes()



def test_result_finalize_still_rejects_runtime_event_chain_drift(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["event_chain_head"] = "f" * 64
    record_path.write_text(canonical_json(record), encoding="utf-8")

    with pytest.raises(ResultContractError, match="无法形成终态运行摘要"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            claim_level="research_observation",
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "research_observation"},
        )



def test_result_assembler_rejects_formal_causal_output_without_parquet(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    (staging / "features").mkdir()
    (staging / "features" / "metadata.json").write_text(
        "{}",
        encoding="utf-8",
    )
    feature = external.commit(
        staging,
        artifact_name="features",
        artifact_type="research.feature-set.v1",
    )
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["outputs"]["feature"] = {
        "features": feature.artifact_ref.to_dict(),
    }
    record_path.write_text(canonical_json(record), encoding="utf-8")
    spec = ResultSpec.build(
        (
            *_spec().tables,
            ResultTableSpec(
                table_id="causal-time.feature.features",
                role="diagnostic",
                source_node_id="feature",
                source_port="features",
                artifact_type="research.feature-set.v1",
                schema_id="research.causal-time.feature.features.v2",
                path_prefix="features",
            ),
        )
    )

    with pytest.raises(ResultContractError, match="没有选中正式 Parquet 表"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=spec,
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            claim_level="research_observation",
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "research_observation"},
        )



def test_result_bundle_reports_atomic_publish_before_reference_write(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    published = []
    assembler = ResultAssembler(result_root)
    kwargs = {
        "run_root": run_root,
        "project_id": "research_package_test",
        "package_hash": HASH_B,
        "plan_hash": HASH_C,
        "result_spec": _spec(),
        "catalog_hashes": {"prices": HASH_D},
        "data_references": {"prices": _data_reference()},
        "metric_proofs": (_proof(),),
        "implementation_manifest_hash": HASH_A,
        "claim_level": "research_observation",
        "formal_input_request_ids": ("prices",),
        "input_claim_ceilings": {"prices": "research_observation"},
        "published_hook": lambda bundle, directory: published.append(
            (bundle.result_id, directory)
        ),
    }

    bundle, directory, _ = assembler.finalize(**kwargs)
    assert published == [(bundle.result_id, directory)]

    published.clear()
    same_bundle, same_directory, _ = assembler.finalize(**kwargs)
    assert same_bundle == bundle
    assert same_directory == directory
    assert published == [(bundle.result_id, directory)]



def test_result_bundle_prepared_failure_never_publishes(tmp_path: Path) -> None:
    run_root, result_root, _object_root = _runtime_fixture(tmp_path)

    def fail_after_prepare(phase: str) -> None:
        if phase == "prepared":
            raise RuntimeError("故障注入：staging 已完整准备")

    with pytest.raises(RuntimeError, match="故障注入"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            claim_level="research_observation",
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "research_observation"},
            phase_hook=fail_after_prepare,
        )
    namespace = result_root / "research_package_test" / HASH_A
    assert not namespace.exists() or not tuple(namespace.iterdir())



def test_result_store_ignores_run_root_changes_after_finalize(tmp_path: Path) -> None:
    run_root, result_root, object_root, bundle, _, _ = _finalize(tmp_path)
    pq.write_table(
        pa.table({"metric": ["alpha"], "value": [999.0]}),
        object_root / "statistics" / "metrics.parquet",
    )
    (object_root / "result.json").write_text('{"tampered":true}', encoding="utf-8")
    store = ResultStore(result_root, create=False)
    table = store.read_table_by_schema_id(
        bundle, schema_id="data.columnar-bundle.metrics.v1"
    )
    assert table["value"].to_pylist() == [1.25]



def test_result_snapshot_reads_partition_schema_with_arrow_metadata(
    tmp_path: Path,
) -> None:
    schema_id = "research.metadata.fixture.v1"
    schema = pa.schema(
        [
            pa.field(
                "value",
                pa.int64(),
                metadata={b"unit": b"count"},
            )
        ],
        metadata={b"contract": b"fixture-v1"},
    )
    paths = ("part-00000.parquet", "part-00001.parquet")
    for index, relative_path in enumerate(paths):
        pq.write_table(
            pa.Table.from_arrays([pa.array([index])], schema=schema),
            tmp_path / relative_path,
        )
    manifest = SimpleNamespace(
        schema_id=schema_id,
        files={relative_path: HASH_A for relative_path in paths},
        row_counts={relative_path: 1 for relative_path in paths},
    )
    snapshot = ResultSnapshot(
        SimpleNamespace(tables=(manifest,)),
        tmp_path,
        {},
        {},
    )

    assert snapshot.table_schema(schema_id) == schema



def test_result_snapshot_counts_nested_projection_from_parquet_footer(
    tmp_path: Path,
) -> None:
    schema_id = "research.nested.fixture.v1"
    relative_path = "nested.parquet"
    table = pa.table({
        "nested": pa.array(
            [{"text": "alpha", "count": 1}, {"text": "beta", "count": 2}],
            type=pa.struct((
                pa.field("text", pa.string()),
                pa.field("count", pa.int64()),
            )),
        ),
        "other": pa.array([100, 200], type=pa.int64()),
    })
    pq.write_table(table, tmp_path / relative_path)
    manifest = SimpleNamespace(
        schema_id=schema_id,
        files={relative_path: HASH_A},
        row_counts={relative_path: table.num_rows},
    )
    snapshot = ResultSnapshot(
        SimpleNamespace(tables=(manifest,)),
        tmp_path,
        {},
        {},
    )
    metadata = pq.ParquetFile(tmp_path / relative_path).metadata
    expected_nested = sum(
        int(metadata.row_group(row_group).column(column).total_uncompressed_size)
        for row_group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(row_group).num_columns)
        if metadata.row_group(row_group)
        .column(column)
        .path_in_schema.split(".", 1)[0]
        == "nested"
    )

    assert snapshot.table_uncompressed_bytes(
        schema_id,
        columns=("nested",),
    ) == expected_nested
    assert 0 < expected_nested < snapshot.table_uncompressed_bytes(schema_id)



def test_result_verify_hashes_in_chunks_and_uses_parquet_footer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_root, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    table_path = (directory / next(iter(bundle.tables[0].files))).resolve()
    original_open = Path.open
    read_sizes: list[int] = []

    class TrackingFile:
        def __init__(self, handle) -> None:
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def read(self, size: int = -1):
            read_sizes.append(size)
            return self._handle.read(size)

        def __getattr__(self, name: str):
            return getattr(self._handle, name)

    def tracked_open(path: Path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.resolve() == table_path and args and args[0] == "rb":
            return TrackingFile(handle)
        return handle

    original_parquet_file = result_store_module.pq.ParquetFile

    class FooterOnlyParquetFile:
        def __init__(self, *args, **kwargs) -> None:
            self._delegate = original_parquet_file(*args, **kwargs)

        @property
        def metadata(self):
            return self._delegate.metadata

        def read(self, *args, **kwargs):
            raise AssertionError("完整 Result 校验不得读取 Parquet 数据页")

    monkeypatch.setattr(result_store_module, "_HASH_CHUNK_BYTES", 32)
    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(
        result_store_module.pq,
        "ParquetFile",
        FooterOnlyParquetFile,
    )

    assert ResultStore(result_root, create=False).verify(directory) == bundle
    assert len(read_sizes) > 1
    assert set(read_sizes) == {32}



def test_result_bundle_rejects_cross_volume_staging_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    assembler = ResultAssembler(result_root)
    original_stat = Path.stat

    class StatWithDifferentDevice:
        def __init__(self, metadata) -> None:
            self._metadata = metadata
            self.st_dev = metadata.st_dev + 1

        def __getattr__(self, name: str):
            return getattr(self._metadata, name)

    def fake_stat(path: Path, *args, **kwargs):
        metadata = original_stat(path, *args, **kwargs)
        if path.parent == assembler.store.staging_root:
            return StatWithDifferentDevice(metadata)
        return metadata

    monkeypatch.setattr(Path, "stat", fake_stat)
    with pytest.raises(ResultContractError, match="不在同一卷"):
        assembler.finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            claim_level="research_observation",
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "research_observation"},
        )
    namespace = result_root / "research_package_test" / HASH_A
    assert not namespace.exists() or not tuple(namespace.iterdir())



def test_result_spec_rejects_path_escape_and_multiple_primary() -> None:
    with pytest.raises(ResultContractError, match="POSIX"):
        ResultTableSpec(
            "bad", "primary", "statistics", "statistics", "data.columnar-bundle.v1",
            "data.columnar-bundle.metrics.v1", "../statistics",
        )
    primary = _spec().tables[0]
    with pytest.raises(ResultContractError, match="恰好"):
        ResultSpec.build((primary, ResultTableSpec(
            "second", "primary", "simulation", "simulation", "research.simulation.v1",
            "project.test-output.v1", "simulation",
        )))



def test_result_bundle_rejects_conflicting_second_finalize(tmp_path: Path) -> None:
    run_root, result_root, _, bundle, _, _ = _finalize(tmp_path)
    with pytest.raises(ResultContractError, match="另一份 canonical ResultBundle"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id=bundle.project_id,
            package_hash=HASH_D,
            plan_hash=bundle.plan_hash,
            result_spec=bundle.result_spec,
            catalog_hashes=bundle.catalog_hashes,
            data_references={"prices": _data_reference()},
            metric_proofs=bundle.metric_proofs,
            implementation_manifest_hash=bundle.implementation_manifest_hash,
            claim_level="research_observation",
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "research_observation"},
        )



@pytest.mark.parametrize("field", ["schema_hashes", "row_counts"])
def test_result_bundle_rejects_rehashed_table_manifest_drift(
    tmp_path: Path,
    field: str,
) -> None:
    run_root, result_root, _, _, directory, _ = _finalize(tmp_path)
    payload = json.loads((directory / "result.json").read_text(encoding="utf-8"))
    table = payload["tables"][0]
    relative_path = next(iter(table[field]))
    if field == "schema_hashes":
        table[field][relative_path] = "f" * 64
    else:
        table[field][relative_path] += 1
    table["table_manifest_hash"] = typed_canonical_hash({
        key: value for key, value in table.items() if key != "table_manifest_hash"
    })
    payload["result_id"] = typed_canonical_hash({
        key: value for key, value in payload.items() if key != "result_id"
    })
    (directory / "result.json").write_text(canonical_json(payload), encoding="utf-8")
    (directory / "COMMITTED").write_text(payload["result_id"], encoding="ascii")
    rewritten = directory.with_name(payload["result_id"])
    directory.rename(rewritten)
    with pytest.raises(ResultContractError, match="schema 或行数漂移"):
        ResultStore(result_root, create=False).verify(rewritten)



def test_result_survives_run_root_deletion_and_reads_each_parquet_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    shutil.rmtree(run_root)
    table_path = directory / next(iter(bundle.tables[0].files))
    original_open = Path.open
    table_opens = 0

    def counted_open(path: Path, *args, **kwargs):
        nonlocal table_opens
        if path == table_path:
            table_opens += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    table = ResultStore(result_root, create=False).read_table_by_schema_id(
        bundle, schema_id="data.columnar-bundle.metrics.v1"
    )
    assert table["value"].to_pylist() == [1.25]
    assert table_opens == 1



def test_result_bundle_supports_two_nodes_and_two_public_table_roles(tmp_path: Path) -> None:
    run_root, result_root = _dual_table_runtime_fixture(tmp_path)
    result_spec = ResultSpec.build((
        ResultTableSpec(
            "diagnostics", "diagnostic", "diagnostics", "diagnostics",
            "research.diagnostics.v1", "research.diagnostics.test.v1", "diagnostics",
        ),
        ResultTableSpec(
            "primary_statistics", "primary", "statistics", "statistics",
            "data.columnar-bundle.v1", "data.columnar-bundle.metrics.v1", "statistics",
        ),
    ))
    bundle, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=result_spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        claim_level="research_observation",
        formal_input_request_ids=("prices",),
        input_claim_ceilings={"prices": "research_observation"},
    )
    assert tuple(table.table_id for table in bundle.tables) == (
        "diagnostics", "primary_statistics",
    )
    assert tuple(table.source_port for table in bundle.tables) == (
        "diagnostics", "statistics",
    )
    assert ResultStore(result_root, create=False).verify(directory) == bundle



def test_result_snapshot_does_not_open_unrequested_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root = _dual_table_runtime_fixture(tmp_path)
    result_spec = ResultSpec.build((
        ResultTableSpec(
            "diagnostics", "diagnostic", "diagnostics", "diagnostics",
            "research.diagnostics.v1", "research.diagnostics.test.v1", "diagnostics",
        ),
        ResultTableSpec(
            "primary_statistics", "primary", "statistics", "statistics",
            "data.columnar-bundle.v1", "data.columnar-bundle.metrics.v1", "statistics",
        ),
    ))
    bundle, _directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=result_spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        claim_level="research_observation",
        formal_input_request_ids=("prices",),
        input_claim_ceilings={"prices": "research_observation"},
    )
    diagnostic_manifest = next(
        item for item in bundle.tables if item.schema_id == "research.diagnostics.test.v1"
    )
    diagnostic_path = (
        ResultStore(result_root, create=False).result_directory(bundle)
        / next(iter(diagnostic_manifest.files))
    ).resolve()
    original_open = Path.open

    def reject_diagnostic_open(path: Path, *args, **kwargs):
        if path.resolve() == diagnostic_path:
            raise AssertionError("未请求的大表不得被打开")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_diagnostic_open)
    snapshot = ResultStore(result_root, create=False).load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        schema_ids=("data.columnar-bundle.metrics.v1",),
        verify_all_files=False,
    )

    assert tuple(snapshot.tables) == ("data.columnar-bundle.metrics.v1",)



def test_result_snapshot_verifies_lazy_table_without_reading_data_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_root, result_root, _, bundle, _directory, _ = _finalize(tmp_path)
    original_parquet_file = result_store_module.pq.ParquetFile

    class FooterOnlyParquetFile:
        def __init__(self, *args, **kwargs) -> None:
            self._delegate = original_parquet_file(*args, **kwargs)

        @property
        def metadata(self):
            return self._delegate.metadata

        def read(self, *args, **kwargs):
            raise AssertionError("lazy schema 校验不得读取 Parquet 数据页")

    monkeypatch.setattr(
        result_store_module.pq,
        "ParquetFile",
        FooterOnlyParquetFile,
    )
    snapshot = ResultStore(result_root, create=False).load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        verify_schema_ids=("data.columnar-bundle.metrics.v1",),
        verify_all_files=False,
    )

    assert not snapshot.tables
    assert snapshot.verified_schema_ids == frozenset({"data.columnar-bundle.metrics.v1"})
    with pytest.raises(ResultContractError, match="未进入本次已验证快照"):
        snapshot.table_schema("research.unrequested.test.v1")



@pytest.mark.parametrize(
    "changes",
    (
        {"value": float("nan")},
        {"value": float("inf")},
        {"sample_start": "not-a-date"},
        {"sample_start": "2026-07-01", "sample_end": "2026-06-30"},
        {"sample_start": "2026-01-01", "sample_end": "2026-06-30T00:00:00+00:00"},
        {"sample_size": 0},
        {"status": "estimated"},
    ),
)
def test_result_metric_decoder_rejects_invalid_facts(tmp_path: Path, changes) -> None:
    _run_root, _result_root, _object_root, bundle, directory, _reference = _finalize(
        tmp_path
    )
    row = {
        "metric_ref": "data.row_count@1.0.0",
        "value": 1.25,
        "unit": "rows",
        "sample_start": "2026-01-01",
        "sample_end": "2026-06-30",
        "sample_size": 120,
        "status": "computed",
    }
    row.update(changes)
    snapshot = ResultSnapshot(
        bundle,
        directory,
        {"data.columnar-bundle.metrics.v1": pa.Table.from_pylist([row])},
        {},
    )
    with pytest.raises(ResultContractError):
        metrics_from_snapshot(snapshot)



def test_result_metric_decoder_rejects_multiple_rows_for_one_metric(tmp_path: Path) -> None:
    _run_root, _result_root, _object_root, bundle, directory, _reference = _finalize(
        tmp_path
    )
    row = {
        "metric_ref": "data.row_count@1.0.0",
        "value": 1.25,
        "unit": "rows",
        "sample_start": "2026-01-01",
        "sample_end": "2026-06-30",
        "sample_size": 120,
        "status": "computed",
    }
    snapshot = ResultSnapshot(
        bundle,
        directory,
        {"data.columnar-bundle.metrics.v1": pa.Table.from_pylist([row, row])},
        {},
    )
    with pytest.raises(ResultContractError, match="恰好有一行"):
        metrics_from_snapshot(snapshot)
