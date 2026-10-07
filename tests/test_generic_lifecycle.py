from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import timedelta
import importlib
import json
import shutil
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import research_pipeline.evidence.verification_result as verification_result_module

from research_pipeline.evidence import (
    compare_verification_results,
    load_verified_result_context,
    render_verification_report,
    export_verified_result,
    verify_result,
)
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.validity_recompute import (
    VALIDITY_FACTS_PRODUCER_HASH,
    policy_id_for_claim,
    recompute_gate_results,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.metric_contracts import (
    MetricReachabilityProof,
    build_mainline_metric_registry,
)
from research_pipeline.results import (
    ResultAssembler,
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
from validity_facts_support import default_passing_validity_facts


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
            "research.validity.data-observation",
        ),
    )
    edge_specs = (
        (0, "admission", 1, "admission"),
        (1, "data", 2, "data"),
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
    validity_facts: dict[str, object] | None = None,
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
        if context.node.implementation_id == "research.validity.data-observation.v1":
            staging = context.external_store.prepare()
            (staging / "result.json").write_text(
                canonical_json(
                    default_passing_validity_facts()
                    if validity_facts is None
                    else validity_facts
                ),
                encoding="utf-8",
            )
            return RuntimeNodeValue.external(context.external_store.commit(
                staging,
                artifact_name="validity",
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
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
    )
    return runtime, run_root, result_root, bundle, result_directory, reference


def _facts_with_stock_cost_assumption() -> dict[str, object]:
    facts = default_passing_validity_facts()
    financial = facts["financial_tradability"]
    assumption = {
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
    financial["research_cost_assumption"] = assumption
    financial["simulation_window"] = {
        "start": "2024-01-02",
        "end": "2024-01-03",
    }
    summary = financial["simulation_semantics"]["summary"]
    summary["cost_model_id"] = "research-cost-assumption"
    summary["cost_model_version"] = "v1"
    for block in financial["simulation_semantics"]["blocks"]:
        for session in block["sessions"]:
            session["cost_model_id"] = "research-cost-assumption"
            session["cost_model_version"] = "v1"
            session["semantics_hash"] = typed_canonical_hash({
                key: value
                for key, value in session.items()
                if key not in {"session", "semantics_hash"}
            })
    return facts


def test_verification_result_survives_run_root_removal(tmp_path, monkeypatch) -> None:
    runtime, run_root, result_root, bundle, result_directory, _reference = _run_to_result(
        tmp_path, "generic.structured-verification"
    )
    verification_path = tmp_path / "verification-result.json"
    context = verify_result(
        result_directory,
        result_store=result_root,
        output=verification_path,
    )
    assert runtime["status"] == "succeeded"
    assert context.snapshot.bundle == bundle
    assert context.metrics[0].to_dict() == {
        "metric_ref": "data.row_count@1.0.0",
        "value": 1.0,
        "unit": "rows",
        "sample_start": "2026-01-01",
        "sample_end": "2026-06-30",
        "sample_size": 120,
        "status": "computed",
        "table_id": "dataset_statistics",
    }

    original = json.loads(verification_path.read_text(encoding="utf-8"))
    for field, value in (
        ("algorithm_version", "verifier.fake.v1"),
        ("result_hash", "0" * 64),
    ):
        tampered = json.loads(json.dumps(original))
        tampered["gate_records"][0][field] = value
        tampered["verification_hash"] = typed_canonical_hash({
            key: item for key, item in tampered.items() if key != "verification_hash"
        })
        tampered_path = tmp_path / f"tampered-{field}.json"
        tampered_path.write_text(canonical_json(tampered), encoding="utf-8")
        with pytest.raises(EvidenceContractError):
            load_verified_result_context(tampered_path, result_store=result_root)

    shutil.rmtree(run_root)
    monkeypatch.setattr(
        verification_result_module,
        "verify_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("report/compare/export 不应重新执行完整 verify")
        ),
    )
    loaded = load_verified_result_context(verification_path, result_store=result_root)
    report = render_verification_report(loaded)
    assert "1.0 rows" in report
    assert "2026-01-01 至 2026-06-30，n=120" in report
    assert compare_verification_results(loaded, loaded).comparable is True
    exported = export_verified_result(loaded, tmp_path / "exported")
    assert ResultStore(tmp_path / "exported", create=False).verify(exported)


def test_stock_cost_assumption_survives_verification_and_report(tmp_path) -> None:
    facts = _facts_with_stock_cost_assumption()
    expected = facts["financial_tradability"]["research_cost_assumption"]
    _, _, result_root, _, result_directory, _ = _run_to_result(
        tmp_path,
        "generic.stock-cost-assumption",
        validity_facts=facts,
    )
    verification_path = tmp_path / "stock-cost-verification.json"

    context = verify_result(
        result_directory,
        result_store=result_root,
        output=verification_path,
    )

    assert context.verification.status == "pass"
    assert context.verification.research_cost_assumption == expected
    assert json.loads(verification_path.read_text(encoding="utf-8"))[
        "research_cost_assumption"
    ] == expected
    report = render_verification_report(context)
    assert "研究假设（不是历史真实费率）" in report
    assert "适用 2024-01-01 至 2024-12-31" in report
    assert "佣金 300 ppm" in report
    assert "最低佣金 500 分" in report
    assert "卖出税 1000 ppm" in report
    assert "过户费 10 ppm" in report
    assert "每股滑点 1 分" in report


def test_stock_cost_assumption_tamper_or_uncovered_window_is_rejected(
    tmp_path,
) -> None:
    facts = _facts_with_stock_cost_assumption()
    _, _, result_root, _, result_directory, _ = _run_to_result(
        tmp_path,
        "generic.stock-cost-rejection",
        validity_facts=facts,
    )
    verification_path = tmp_path / "stock-cost-verification.json"
    verify_result(
        result_directory,
        result_store=result_root,
        output=verification_path,
    )

    tampered = json.loads(verification_path.read_text(encoding="utf-8"))
    tampered["research_cost_assumption"]["commission_ppm"] = 1
    tampered["verification_hash"] = typed_canonical_hash({
        key: value
        for key, value in tampered.items()
        if key != "verification_hash"
    })
    tampered_path = tmp_path / "tampered-stock-cost-verification.json"
    tampered_path.write_text(canonical_json(tampered), encoding="utf-8")
    with pytest.raises(EvidenceContractError, match="绑定不一致"):
        load_verified_result_context(tampered_path, result_store=result_root)

    uncovered = _facts_with_stock_cost_assumption()
    uncovered["financial_tradability"]["research_cost_assumption"][
        "applicable_end"
    ] = "2024-01-02"
    _, _, uncovered_store, _, uncovered_result, _ = _run_to_result(
        tmp_path,
        "generic.stock-cost-uncovered",
        validity_facts=uncovered,
    )
    uncovered_context = verify_result(
        uncovered_result,
        result_store=uncovered_store,
    )
    assert uncovered_context.verification.status == "fail"
    financial_gate = next(
        item
        for item in uncovered_context.verification.gate_records
        if item.gate_id == "financial.tradability"
    )
    assert {item.code for item in financial_gate.findings} >= {
        "financial.capacity_semantics_invalid"
    }


def test_result_export_stream_verification_rejects_source_tamper(tmp_path) -> None:
    _, _, result_root, bundle, result_directory, _ = _run_to_result(
        tmp_path,
        "generic.export-integrity",
    )
    verification_path = tmp_path / "verification-result.json"
    verify_result(
        result_directory,
        result_store=result_root,
        output=verification_path,
    )
    context = load_verified_result_context(
        verification_path,
        result_store=result_root,
    )
    table_path = result_directory / next(iter(bundle.tables[0].files))
    with table_path.open("ab") as handle:
        handle.write(b"tampered-after-verification")

    destination = tmp_path / "tampered-export"
    with pytest.raises(ResultContractError, match="hash 漂移"):
        export_verified_result(context, destination)
    assert not destination.exists()
    assert not (tmp_path / ".tampered-export.tmp").exists()


def test_verified_result_consumer_rejects_required_metric_table_tamper(
    tmp_path,
) -> None:
    _, _, result_root, bundle, result_directory, _ = _run_to_result(
        tmp_path,
        "generic.report-integrity",
    )
    verification_path = tmp_path / "verification-result.json"
    verify_result(
        result_directory,
        result_store=result_root,
        output=verification_path,
    )
    metric_schema_ids = {
        proof.result_schema_id for proof in bundle.metric_proofs
    }
    metric_manifest = next(
        table for table in bundle.tables if table.schema_id in metric_schema_ids
    )
    metric_path = result_directory / next(iter(metric_manifest.files))
    with metric_path.open("ab") as handle:
        handle.write(b"tampered-after-verification")

    with pytest.raises(ResultContractError, match="hash 漂移"):
        load_verified_result_context(
            verification_path,
            result_store=result_root,
        )


def test_verifier_publishes_failed_validity_as_a_verification_result(
    tmp_path,
) -> None:
    facts = default_passing_validity_facts()
    facts["data_pit"]["observations"][0]["available_at_ns"] = 101
    facts["data_pit"]["observations"][0]["decision_at_ns"] = 100
    _, _, result_root, _, result_directory, _ = _run_to_result(
        tmp_path,
        "generic.failed-validity",
        validity_facts=facts,
    )
    output = tmp_path / "failed-verification-result.json"

    context = verify_result(
        result_directory,
        result_store=result_root,
        output=output,
    )

    assert context.verification.status == "fail"
    assert context.verification.integrity_status == "pass"
    assert context.verification.reproducibility_status == "pass"
    assert context.verification.validity_status == "fail"
    pit_gate = next(
        item
        for item in context.verification.gate_records
        if item.gate_id == "data.pit"
    )
    assert pit_gate.status == "fail"
    assert [item.code for item in pit_gate.findings] == ["pit.future_data"]
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "fail"
    loaded = load_verified_result_context(output, result_store=result_root)
    assert "pass / pass / fail" in render_verification_report(loaded)
    assert compare_verification_results(loaded, loaded).comparable
    exported = export_verified_result(loaded, tmp_path / "failed-exported")
    assert ResultStore(tmp_path / "failed-exported", create=False).verify(
        exported
    )


def test_two_projects_keep_result_and_verification_identities_isolated(tmp_path) -> None:
    contexts = []
    stores = []
    verification_paths = []
    result_directories = []
    bundles = []
    for project_id in ("generic.alpha", "generic.beta"):
        _, _, result_root, bundle, result_directory, _ = _run_to_result(tmp_path, project_id)
        verification_path = tmp_path / project_id / "verification-result.json"
        context = verify_result(
            result_directory,
            result_store=result_root,
            output=verification_path,
        )
        assert context.verification.result_reference.project_id == project_id
        contexts.append(context)
        stores.append(result_root)
        verification_paths.append(verification_path)
        result_directories.append(result_directory)
        bundles.append(bundle)

    assert bundles[0].package_hash != bundles[1].package_hash
    assert bundles[0].run_id != bundles[1].run_id
    assert bundles[0].result_id != bundles[1].result_id
    assert contexts[0].verification.verification_hash != contexts[1].verification.verification_hash
    with pytest.raises(ResultContractError):
        load_verified_result_context(verification_paths[0], result_store=stores[1])

    validity_path = (
        result_directories[0] / bundles[0].verification.validity_source_path
    )
    with validity_path.open("ab") as handle:
        handle.write(b"tampered-validity")
    with pytest.raises(ResultContractError):
        verify_result(result_directories[0], result_store=stores[0])
