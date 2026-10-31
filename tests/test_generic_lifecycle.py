from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import date
import importlib
import shutil
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.metric_contracts import (
    MetricReachabilityProof,
    build_mainline_metric_registry,
)
from research_pipeline.results import (
    ResultAssembler,
    load_result_context,
    render_result_report,
    build_result_report,
    compare_results,
    export_result,
    ResultContractError,
    ResultSpec,
    ResultStore,
    ResultTableSpec,
)
from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    CheckpointPolicy,
    DagSpec,
    Edge,
    NodeSpec,
    PartitionSpec,
    ResourceBudget,
    RetryPolicy,
    RuntimeExecutionService,
    RuntimeNodeValue,
)
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest


def _dag_from_operator_ids(dag_id: str, operator_ids: tuple[str, ...]) -> tuple[DagSpec, list[NodeSpec]]:
    manifest = build_mainline_operator_manifest()
    nodes: list[NodeSpec] = []
    for operator_id in operator_ids:
        definition = manifest.require_operator(operator_id, "1.0.0")
        nodes.append(NodeSpec(
            operator_id,
            definition.implementation_ref.implementation_id,
            tuple((item.port, item.artifact_type) for item in definition.input_schema),
            tuple((item.port, item.artifact_type) for item in definition.output_schema),
            ResourceBudget(1024 * 1024, 1, 1024 * 1024, 30),
            RetryPolicy(2, ("operator_execution_failed",)),
            CheckpointPolicy.REQUIRED,
            PartitionSpec(False),
            configuration_hash=typed_canonical_hash({"dag": dag_id, "operator": operator_id}),
        ))
    return DagSpec(dag_id, tuple(nodes), ()), nodes


def _formal_dag() -> DagSpec:
    dag, nodes = _dag_from_operator_ids(
        "dataset_observation_without_simulation",
        (
            "data.catalog.admission",
            "data.columnar.materialize",
        ),
    )
    edge_specs = (
        (0, "admission", 1, "admission"),
    )
    edges = tuple(
        Edge(
            nodes[source].node_id,
            source_port,
            nodes[target].node_id,
            target_port,
            dict(nodes[source].output_types)[source_port],
        )
        for source, source_port, target, target_port in edge_specs
    )
    return DagSpec(dag.dag_id, dag.nodes, edges)


def _runtime_service() -> RuntimeExecutionService:
    return RuntimeExecutionService(
        audit_environment=AuditEnvironmentManifest.capture(
            build_artifact_digest="a" * 64,
            dependency_distribution_digests={"numpy": "b" * 64},
        ),
        numerical_backend_names=("numpy",),
    )


@contextmanager
def _patched_runtime_adapters(dag: DagSpec, action):
    """测试只替换正式 adapter 函数，不向生产 Runtime 暴露 callable 入口。"""
    definitions = {
        item.implementation_ref.implementation_id: item
        for item in build_mainline_operator_manifest().definitions
    }
    with ExitStack() as stack:
        for node in dag.nodes:
            reference = definitions[node.implementation_id].runtime_adapter_ref
            module = importlib.import_module(reference.module_name)
            stack.enter_context(
                patch.object(
                    module,
                    reference.symbol_name,
                    lambda context, action=action: action(context.node_context),
                )
            )
        yield


def _data_reference() -> dict[str, object]:
    return {
        "relative_path": "objects/prices",
        "physical_snapshot_id": "a" * 64,
        "manifest_hash": "b" * 64,
        "schema_hash": "c" * 64,
        "source_revision_hash": "d" * 64,
        "partitions": ["date=2026-01-01/part.parquet"],
        "contract_version": "dataset-artifact-ref-v1",
    }


def _run_to_result(
    tmp_path,
    project_id: str,
    *,
    implementation_manifest_hash: str | None = None,
    package_hash: str | None = None,
    plan_hash: str | None = None,
    research_cost_assumption: dict[str, object] | None = None,
    metric_sample_start: str = "2026-01-01",
    analysis_table: pa.Table | None = None,
):
    dag = _formal_dag()

    def action(context):
        if context.node.implementation_id == "data.columnar.materialize.v1":
            staging = context.external_store.prepare()
            (staging / "statistics").mkdir()
            pq.write_table(
                pa.table({
                    "metric_ref": ["data.row_count@1.0.0"],
                    "value": [1.0],
                    "unit": ["rows"],
                    "sample_start": [metric_sample_start],
                    "sample_end": ["2026-06-30"],
                    "sample_size": pa.array([120], type=pa.int64()),
                    "status": ["computed"],
                }),
                staging / "statistics" / "metrics.parquet",
            )
            if analysis_table is not None:
                (staging / "analysis").mkdir()
                pq.write_table(
                    analysis_table,
                    staging / "analysis" / "series.parquet",
                )
            (staging / "result.json").write_text("{}", encoding="utf-8")
            return RuntimeNodeValue.external(context.external_store.commit(
                staging,
                artifact_name="data",
                artifact_type=context.node.output_types[0][1],
            ))
        return RuntimeNodeValue.inline(
            name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
            content=f"{context.node.node_id}:{project_id}".encode(),
        )

    run_root = tmp_path / project_id / "run"
    with _patched_runtime_adapters(dag, action):
        runtime = _runtime_service().execute(
            dag=dag,
            environment=None,
            run_root=run_root,
            project_id=project_id,
            root_seed=7,
            fixed_clock="2026-07-18T00:00:00+08:00",
        )
    definition = build_mainline_metric_registry().require(
        "data.row_count@1.0.0"
    )
    proof = MetricReachabilityProof.build(
        definition,
        ("data.columnar.materialize", "data", "data.columnar-bundle.v1"),
        ("dataset_statistics", "data.columnar-bundle.metrics.v1", "statistics"),
    )
    tables = [ResultTableSpec(
        "dataset_statistics",
        "primary",
        "data.columnar.materialize",
        "data",
        "data.columnar-bundle.v1",
        "data.columnar-bundle.metrics.v1",
        "statistics",
    )]
    if analysis_table is not None:
        tables.append(ResultTableSpec(
            "analysis_series",
            "diagnostic",
            "data.columnar.materialize",
            "data",
            "data.columnar-bundle.v1",
            "research.generic-analysis-series.v1",
            "analysis",
        ))
    result_spec = ResultSpec.build(tuple(sorted(
        tables,
        key=lambda item: item.table_id,
    )))
    result_root = tmp_path / project_id / "results"
    bundle, result_directory, reference = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id=project_id,
        package_hash=(
            package_hash or typed_canonical_hash({"project_id": project_id})
        ),
        plan_hash=plan_hash or "c" * 64,
        result_spec=result_spec,
        catalog_hashes={"prices": "a" * 64},
        data_references={"prices": _data_reference()},
        metric_proofs=(proof,),
        implementation_manifest_hash=(
            implementation_manifest_hash
            or build_mainline_operator_manifest().manifest_hash
        ),
        claim_level="research_observation",
        formal_input_request_ids=("prices",),
        input_claim_ceilings={"prices": "research_observation"},
        research_cost_assumption=research_cost_assumption,
    )
    return runtime, run_root, result_root, bundle, result_directory, reference


def _stock_cost_assumption() -> dict[str, object]:
    return {
        "contract_version": "research-cost-assumption-v1",
        "assumption_id": "fixture-cn-stock-cost-v1",
        "currency": "CNY",
        "rate_unit": "ppm_of_notional",
        "minimum_fee_unit": "CNY_cent",
        "slippage_unit": "CNY_cent_per_share",
        "applicable_start": "2024-01-01",
        "applicable_end": "2024-12-31",
        "commission_ppm": 300,
        "min_commission_units": 500,
        "sell_tax_ppm": 1000,
        "transfer_fee_ppm": 10,
        "slippage_per_share_units": 1,
    }


def test_result_consumers_survive_run_root_removal(tmp_path, monkeypatch) -> None:
    runtime, run_root, result_root, bundle, directory, _ = _run_to_result(
        tmp_path, "generic.direct-result"
    )
    assert runtime["status"] == "succeeded"
    context = load_result_context(directory, result_store=result_root)
    assert context.snapshot.bundle == bundle
    assert context.metrics[0].to_dict() == {
        "metric_ref": "data.row_count@1.0.0", "value": 1.0, "unit": "rows",
        "sample_start": "2026-01-01", "sample_end": "2026-06-30",
        "sample_size": 120, "status": "computed", "table_id": "dataset_statistics",
    }
    assert context.snapshot.bundle.run.status == "succeeded"
    assert context.claim_level == "research_observation"
    assert not hasattr(context, "verification")
    shutil.rmtree(run_root)

    def unexpected_run(*args, **kwargs):
        pytest.fail("Result 消费不得重新执行研究")

    monkeypatch.setattr(RuntimeExecutionService, "execute", unexpected_run)
    loaded = load_result_context(directory, result_store=result_root)
    report = render_result_report(loaded)
    assert "1.0 rows" in report
    assert "2026-01-01 至 2026-06-30，n=120" in report
    assert compare_results(loaded, loaded).comparable is True
    exported = export_result(loaded, tmp_path / "exported")
    assert ResultStore(tmp_path / "exported", create=False).verify(exported) == bundle


def test_stock_cost_assumption_survives_result_and_report(tmp_path) -> None:
    expected = _stock_cost_assumption()
    _, _, store, _, directory, _ = _run_to_result(
        tmp_path, "generic.stock-cost-assumption", research_cost_assumption=expected,
    )
    context = load_result_context(directory, result_store=store)
    assert dict(context.research_cost_assumption) == expected
    assert build_result_report(context).to_dict()["research_cost_assumption"] == expected
    report = render_result_report(context)
    assert "研究费用假设" in report
    for field, value in expected.items():
        assert field in report
        assert str(value) in report


@pytest.mark.parametrize("window", [
    {"applicable_start": "2024-01-03"},
    {"applicable_end": "2024-01-02"},
])
def test_stock_execution_rejects_cost_assumption_outside_research_window(window) -> None:
    from research_pipeline.platform.research_cost_assumption import (
        ResearchCostAssumptionError, normalize_stock_research_cost_assumption,
    )

    assumption = {**_stock_cost_assumption(), **window}
    with pytest.raises(ResearchCostAssumptionError, match="未覆盖完整研究窗口"):
        normalize_stock_research_cost_assumption(
            assumption, research_start=date(2024, 1, 2), research_end=date(2024, 1, 3),
        )


def test_result_export_rejects_source_tamper_and_cleans_partial_output(tmp_path) -> None:
    _, _, store, bundle, directory, _ = _run_to_result(tmp_path, "generic.export-integrity")
    context = load_result_context(directory, result_store=store)
    table_path = directory / next(iter(bundle.tables[0].files))
    with table_path.open("ab") as handle:
        handle.write(b"changed-content")
    destination = tmp_path / "changed-export"
    with pytest.raises(ResultContractError, match="hash 漂移"):
        export_result(context, destination)
    assert not destination.exists()
    assert not (tmp_path / ".changed-export.tmp").exists()


def test_result_consumer_rejects_required_metric_table_tamper(tmp_path) -> None:
    _, _, store, bundle, directory, _ = _run_to_result(tmp_path, "generic.report-integrity")
    metric_schema_ids = {proof.result_schema_id for proof in bundle.metric_proofs}
    manifest = next(table for table in bundle.tables if table.schema_id in metric_schema_ids)
    with (directory / next(iter(manifest.files))).open("ab") as handle:
        handle.write(b"changed-content")
    with pytest.raises(ResultContractError, match="hash 漂移"):
        load_result_context(directory, result_store=store)


def test_two_projects_keep_result_identities_isolated(tmp_path) -> None:
    contexts, stores, bundles, directories = [], [], [], []
    for project_id in ("generic.alpha", "generic.beta"):
        _, _, store, bundle, directory, _ = _run_to_result(tmp_path, project_id)
        context = load_result_context(directory, result_store=store)
        assert context.snapshot.bundle.run.project_id == project_id
        contexts.append(context)
        stores.append(store)
        bundles.append(bundle)
        directories.append(directory)
    assert bundles[0].package_hash != bundles[1].package_hash
    assert bundles[0].run_id != bundles[1].run_id
    assert bundles[0].result_id != bundles[1].result_id
    with pytest.raises(ResultContractError):
        load_result_context(directories[0], result_store=stores[1])
