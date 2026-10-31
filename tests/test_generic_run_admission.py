from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from observation_package_support import copy_observation_package
import research_pipeline.packages as packages_module
from research_pipeline.application import research_package, research_run
from research_pipeline.application.research_plan_store import (
    OPERATOR_GRAPH_PLAN_VERSION,
    PLAN_MANIFEST,
    load_operator_graph_research_plan,
    publish_operator_graph_research_plan,
    _validate_admitted_query_facts,
)
from research_pipeline.data_plane import (
    AdmittedQueryPlan,
    PathRolePolicy,
    SelectionClock,
    TemporalSelectionPlan,
)
from research_pipeline.packages import (
    compile_research_package,
    load_research_package,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.runtime.operator_registry import (
    build_mainline_operator_registry,
    compile_operator_graph_dag,
)
from test_data_plane_provider_contract import (
    _data_plane_execution_budget,
    _execution_estimate,
    _plan as _provider_plan,
)


ROOT = Path(__file__).resolve().parents[1]
def _manifest() -> dict[str, object]:
    estimate = _execution_estimate(_provider_plan())
    return {
        "contract_version": OPERATOR_GRAPH_PLAN_VERSION,
        "plan_kind": "operator_graph_research_run_candidate",
        "research_id": "generic.minimum_research",
        "package_id": "research_package_aaaaaaaaaaaaaaaa",
        "package_hash": "a" * 64,
        "package_plan_hash": "b" * 64,
        "registry_hash": "c" * 64,
        "admission_hash": "d" * 64,
        "dag_hash": "e" * 64,
        "fixed_clock": "2026-07-18T00:00:00+08:00",
        "root_seed": 20260718,
        "admitted_plan_hashes": {"prices": "1" * 64},
        "execution_estimates": {"prices": estimate.to_dict()},
    }


def _admitted(*, as_of: str | None = "2026-07-17", catalog_hash: str | None = None) -> dict[str, object]:
    provider_plan = _provider_plan()
    return {
        "prices": SimpleNamespace(
            plan_hash="1" * 64,
            catalog_hash="2" * 64 if catalog_hash is None else catalog_hash,
            attestation_hash="3" * 64,
            availability_policy_hash="4" * 64,
            revision_policy_hash="5" * 64,
            query=SimpleNamespace(
                as_of=as_of,
                to_dict=provider_plan.query.to_dict,
            ),
            object_name=provider_plan.object_name,
            input_claim_ceiling="tradable_simulation",
        )
    }


def _admit_query(query) -> AdmittedQueryPlan:
    fields = tuple(query.field_ids)
    return AdmittedQueryPlan(
        query=query,
        catalog_hash="2" * 64,
        binding_id="binding",
        binding_version=1,
        attestation_hash="3" * 64,
        source_profile="source",
        environment="prod",
        expected_schema_revision="schema-v1",
        availability_policy_hash="4" * 64,
        revision_policy_hash="5" * 64,
        object_name="readonly_object",
        columns=tuple((field, f"c{index}") for index, field in enumerate(fields)),
        field_types=tuple((field, "string") for field in fields),
        field_nullables=tuple((field, False) for field in fields),
        primary_key=(fields[0],),
        event_time_field=fields[0],
        instrument_field=fields[1] if len(fields) > 1 else fields[0],
        temporal_selection=TemporalSelectionPlan(
            SelectionClock("query_as_of"),
            fields[0],
            fields,
            fields,
            tuple((field, "string") for field in fields),
        ),
        input_claim_ceiling="tradable_simulation",
    )


def test_generic_run_with_explicit_observation_package(tmp_path: Path, monkeypatch) -> None:
    package_root = tmp_path / "generic_package"
    copy_observation_package(package_root)
    spec_path = package_root / "spec/research.yaml"
    spec_path.write_text(
        spec_path.read_text(encoding="utf-8").replace(
            "cn_stock_daily_observation_v1",
            "generic.minimum_research",
            1,
        ),
        encoding="utf-8",
    )
    package = load_research_package(package_root)
    registry = build_mainline_operator_registry()
    plan = compile_research_package(package, admission=registry)
    admitted = {
        request_id: _admit_query(query)
        for request_id, query in zip(plan.request_ids, plan.queries, strict=True)
    }
    execution_budget = _data_plane_execution_budget(plan, registry)
    execution_estimates = {
        request_id: _execution_estimate(admitted_plan, execution_budget)
        for request_id, admitted_plan in admitted.items()
    }
    plan = compile_research_package(
        package,
        admission=registry,
        platform_admission={
            "catalog_hash": "2" * 64,
            "pit_contract_hash": "6" * 64,
            "drift_proof_hash": "7" * 64,
            "execution_estimates_hash": typed_canonical_hash(
                {
                    request_id: estimate.to_dict()
                    for request_id, estimate in sorted(execution_estimates.items())
                }
            ),
        },
    )
    metric = {"contract_hash": package.metric_contract.contract_hash, **package.metric_contract.payload()}
    claim = {"contract_hash": package.claim_contract.contract_hash, **package.claim_contract.payload()}
    plan_root = publish_operator_graph_research_plan(
        tmp_path / "plan",
        package_id=package.package_id,
        plan=plan,
        research_identity=package.research_identity_payload(plan_hash=plan.plan_hash),
        metric_contract=metric,
        claim_contract=claim,
        admitted_plans=admitted,
        execution_estimates=execution_estimates,
        dag=compile_operator_graph_dag(plan, registry),
    ).directory
    database = tmp_path / "data.duckdb"
    database.write_bytes(b"read-only-fixture")
    handoff = tmp_path / "handoff.json"

    args = Namespace(
        plan=str(plan_root),
        data_db=str(database),
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(handoff),
        run_root=str(tmp_path / "run"),
        result_store=str(tmp_path / "results"),
        draft_out=str(tmp_path / "draft"),
        mode="bounded_parallel",
        workers=2,
        execution_engine="legacy",
        root_seed=plan.root_seed,
        clock=plan.fixed_clock,
        contract_fixture=False,
        acceptance_proof=None,
        json=True,
    )

    assert plan.research_id == "generic.minimum_research"
    assert (plan_root / "research-plan.json").is_file()

    legacy_root = tmp_path / "legacy-plan"
    shutil.copytree(plan_root, legacy_root)
    legacy_manifest_path = legacy_root / PLAN_MANIFEST
    legacy_manifest = json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
    legacy_manifest.pop("manifest_hash")
    legacy_manifest.pop("node_identity_projection")
    legacy_manifest["contract_version"] = "research-cli-operator-graph-plan-v4"
    legacy_manifest_path.write_text(
        canonical_json(
            {
                **legacy_manifest,
                "manifest_hash": typed_canonical_hash(legacy_manifest),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="版本无效"):
        load_operator_graph_research_plan(target=legacy_root)

    with pytest.raises(ValueError, match="execution_engine.*已删除"):
        research_run._execute(args)
    assert not (tmp_path / "run/local-run-result.json").exists()


def test_validated_plan_rejects_missing_pit_and_future_clock() -> None:
    with pytest.raises(ValueError, match="PIT as_of"):
        _validate_admitted_query_facts(_manifest(), _admitted(as_of=None))
    with pytest.raises(ValueError, match="晚于固定时钟"):
        _validate_admitted_query_facts(_manifest(), _admitted(as_of="2026-07-19"))


def test_validated_plan_keeps_query_estimate_alignment() -> None:
    admitted = _admitted()
    manifest = _manifest()
    _validate_admitted_query_facts(manifest, admitted)
    admitted["prices"].object_name = "other"
    with pytest.raises(ValueError, match="QueryPlan"):
        _validate_admitted_query_facts(manifest, admitted)


@pytest.mark.parametrize("field", ["catalog_hash", "attestation_hash", "availability_policy_hash", "revision_policy_hash"])
def test_validated_plan_rejects_missing_catalog_and_pit_identity(field) -> None:
    admitted = _admitted()
    setattr(admitted["prices"], field, None)
    with pytest.raises(ValueError, match=field):
        _validate_admitted_query_facts(_manifest(), admitted)


def test_package_export_does_not_require_study_reproduction_proof(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import research_pipeline.results as results_module

    package_path = tmp_path / "package"
    package_path.mkdir()
    package = SimpleNamespace(
            package_id="research_package_aaaaaaaaaaaaaaaa",
            package_hash="a" * 64,
            source_provenance_hash="e" * 64,
        spec_payload={
            "research_id": "example.locked_study",
        },
        metric_contract=SimpleNamespace(contract_hash="c" * 64),
        claim_contract=SimpleNamespace(contract_hash="d" * 64),
    )
    plan = SimpleNamespace(plan_hash="b" * 64, queries=(object(),))
    monkeypatch.setattr(packages_module, "load_research_package", lambda _, **_kwargs: package)
    monkeypatch.setattr(research_package, "_compile_trusted_context", lambda *_: plan)
    monkeypatch.setattr(
        packages_module,
        "verify_package_source_provenance",
        lambda *_: {"sources": []},
    )
    monkeypatch.setattr(PathRolePolicy, "validate", lambda *_, **__: None)
    context = object()
    output = tmp_path / "output"
    monkeypatch.setattr(
        results_module,
        "load_result_context",
        lambda *_args, **_kwargs: context,
    )
    monkeypatch.setattr(
        results_module,
        "export_result",
        lambda received, destination: output
        if received is context and Path(destination) == output
        else (_ for _ in ()).throw(AssertionError("export 参数无效")),
    )
    monkeypatch.setattr(research_package, "_validate_package_result", lambda *_: None)
    args = Namespace(
        package_command="export-result",
        package=str(package_path),
        result=str(tmp_path / "result"),
        result_store=str(tmp_path / "result-store"),
        output=str(tmp_path / "output"),
        source_archive_root=None,
    )

    result = research_package._execute(args)

    assert result["output"] == str(output)
