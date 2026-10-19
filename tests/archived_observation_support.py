"""公开测试共用的合成封存来源、审批 Catalog 和观察研究包。"""
from datetime import date
import json

import pyarrow as pa

from research_pipeline.catalog import (
    ApprovalDecision, CatalogCoverageBaseline, CatalogSourceManifest,
    DatasetContract, FieldContract, PhysicalBindingContract, PolicyContract, compile_catalog,
)
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory
from research_pipeline.data_plane import (
    DataPlaneExecutionBudget, DateRangeV1, QueryIR, QueryPurpose, QueryBudget, SortKey,
    UniverseSelection, admit_query, build_logical_snapshot, publish_parquet_snapshot,
)
from research_pipeline.data_plane.dataset_artifacts import build_dataset_artifact_ref
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision


def _compile(tmp_path, schema, *, status="approved"):
    policies = (
        PolicyContract("available.static.v1", 1, "availability", {"claim_ceiling": "research_observation"}),
        PolicyContract("drift.strict.v1", 1, "schema_drift", {"schema_changed": "reject"}),
        PolicyContract("revision.none.v1", 1, "revision", {"mode": "none"}),
    )
    fields = tuple(FieldContract(name, logical, 1, dtype, semantic, unit, False,
        "available.static.v1", ("daily",), ("equity",))
        for name, logical, dtype, semantic, unit in (
            ("fld_date", "listing.date", "date32", "date", "day"),
            ("fld_code", "instrument.id", "string", "identifier", "dimensionless"),
            ("fld_value", "market.value", "float64", "numeric", "dimensionless"),
        ))
    dataset = DatasetContract("archive.test", 1, "cn_stock", "equity", "daily",
        ("fld_code",), tuple(field.field_id for field in fields), "fld_date",
        "available.static.v1", "drift.strict.v1", ("fld_code",))
    inventory = PhysicalInventory("parquet", "archive_test", "test", "snapshot",
        tuple(PhysicalColumn(f.name, str(f.type), f.nullable, i) for i, f in enumerate(schema)))
    old = PhysicalBindingContract("original", 1, dataset.dataset_id, 1, "source", "test",
        "old_table", "a" * 64, "drift.strict.v1",
        {f.field_id: {"kind": "direct", "column": f.field_id} for f in fields})
    archived = PhysicalBindingContract("archived", 1, dataset.dataset_id, 1, "archive_test", "test",
        "snapshot", inventory.schema_revision, "drift.strict.v1", dict(old.column_bindings), status=status)
    contracts = (*policies, *fields, dataset, old, archived)
    names = {FieldContract: ("field", "field_id"), DatasetContract: ("dataset", "dataset_id"),
        PolicyContract: ("policy", "policy_id"), PhysicalBindingContract: ("physical_binding", "binding_id")}
    decisions = tuple(ApprovalDecision(names[type(c)][0], getattr(c, names[type(c)][1]), c.content_hash,
        "approved", "合成来源验收", (), "test", "2026-10-01T00:00:00Z") for c in contracts)
    baseline = CatalogCoverageBaseline("test.v1", 1, ("test",))
    decisions += (ApprovalDecision("coverage_slot", "test", None, "approved", "合成来源验收", (), "test", "2026-10-01T00:00:00Z"),)
    manifest = CatalogSourceManifest("test.v1", 1, baseline.content_hash, None, ("test",), (), {}, {"test": (dataset.dataset_id,)})
    catalog = compile_catalog(baseline=baseline, expected_baseline_hash=baseline.content_hash,
        manifest=manifest, contracts=contracts, decisions=decisions, release_root=tmp_path / "catalog")
    return catalog


def _case(tmp_path):
    from research_pipeline.catalog import DriftAttestation
    table = pa.table({"fld_date": [date(2025, 2, 3), date(2025, 1, 2)],
        "fld_code": ["000001.XSHE", "600000.XSHG"], "fld_value": [2.0, 1.0]})
    catalog = _compile(tmp_path, table.schema)
    binding = catalog.bindings["original"]
    policy = PolicyContract(**dict(catalog.policies["drift.strict.v1"]))
    attestation = DriftAttestation(catalog.catalog_hash, "original", "source", "test", "a" * 64, "a" * 64, policy.content_hash, True)
    query = QueryIR("archive.test", 1, tuple(table.column_names), QueryPurpose.UNIVERSE,
        DateRangeV1(date(2025, 1, 1), date(2025, 3, 1)), UniverseSelection(("000001.XSHE", "600000.XSHG")),
        (), (SortKey("fld_code"),), QueryBudget(100, 1_000_000, 2), adjustment="unadjusted", as_of="2025-03-01")
    original = admit_query(query, catalog=catalog, binding=binding, attestation=attestation)
    revision = SourceRevision("synthetic", "synthetic", (SourceFileEvidence("in-memory", 1, 1),), "a" * 64, None, "metadata")
    root = tmp_path / "inputs"
    snapshot = publish_parquet_snapshot(batches=table.to_batches(max_chunksize=1), schema=table.schema,
        plan=original, logical_snapshot=build_logical_snapshot(original, source_revisions=(revision,),
        output_schema=table.schema, provider_version="synthetic-v1", compiler_version="synthetic-v1"), root=root)
    reference = build_dataset_artifact_ref(snapshot, allowed_root=root)
    manifest = {"contract_version": "archived-input-manifest-v1", "requests": {"input": {
        "kind": "dataset", "original_plan": original.to_dict(), "root": str(root),
        "reference": reference.to_dict(), "binding_id": "archived"}}}
    budget = DataPlaneExecutionBudget(256 * 1024 * 1024, 0, 1)
    return catalog, original, manifest, budget


def build_archived_observation_fixture(root):
    """提供正式 CLI/RD 验收使用的小型合成包、审批 Catalog 和封存来源。"""
    import yaml
    import shutil
    from pathlib import Path

    root.mkdir(parents=True, exist_ok=True)
    catalog, original, manifest, budget = _case(root)
    public_package = Path(__file__).resolve().parents[1] / "examples/equity_cross_section/package"
    package_root = Path(shutil.copytree(public_package, root / "package"))
    package_file = package_root / "package.yaml"
    package = yaml.safe_load(package_file.read_text(encoding="utf-8"))
    package.update(package_slug="archived_observation_fixture", display_name="封存输入合成观察",
        metric_contract={"contract_id": "archived_observation_metrics", "version": "1.0.0",
            "metrics": ["data.row_count@1.0.0"],
            "semantics": {"data.row_count@1.0.0": "封存来源声明范围内的记录数"}})
    package_file.write_text(yaml.safe_dump(package, allow_unicode=True, sort_keys=False), encoding="utf-8")
    spec_file = package_root / "spec/research.yaml"
    spec = yaml.safe_load(spec_file.read_text(encoding="utf-8"))
    request = dict(original.query.to_dict())
    request.pop("ir_version")
    request.pop("as_of")
    request["request_id"] = "input"
    spec.update(research_id="archived_observation_fixture", as_of="2025-03-01",
        fixed_clock="2025-03-02T00:00:00+08:00", requests=[request])
    spec["result"] = {"contract_version": "research-result-spec-v1", "tables": [{
        "table_id": "observation_metrics", "role": "primary", "source_node_id": "data_plane",
        "source_port": "data", "artifact_type": "data.columnar-bundle.v1",
        "schema_id": "data.columnar-bundle.metrics.v1", "path_prefix": "observation"}]}
    spec["graph"] = {"contract_version": "research-operator-graph-recipe-v1",
        "graph_id": "archived_observation_fixture", "nodes": [
            {"node_id": "data_plane", "operator_id": "data.columnar.materialize",
                "operator_version": "1.0.0", "inputs": [], "parameters": {}, "strategies": []},
            {"node_id": "validity", "operator_id": "research.validity.data-observation",
                "operator_version": "1.0.0", "inputs": [{"input_port": "data",
                    "source_node_id": "data_plane", "source_output_port": "data"}],
                "parameters": {}, "strategies": []}]}
    spec_file.write_text(yaml.safe_dump(spec, allow_unicode=True, sort_keys=False), encoding="utf-8")
    manifest_path = root / "input-snapshot-manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return {"package": package_root, "catalog_lock": root / "catalog",
        "input_snapshot_manifest": manifest_path, "clock": spec["fixed_clock"], "root_seed": spec["root_seed"],
        "catalog": catalog, "original_plan": original, "execution_budget": budget}
