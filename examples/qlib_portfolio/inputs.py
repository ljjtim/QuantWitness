"""合成ETF的封存Parquet及独立Catalog，不连接数据库。"""
from dataclasses import replace
from pathlib import Path
import json
import pyarrow as pa

from research_pipeline.catalog import (
    ApprovalDecision, CatalogCoverageBaseline, CatalogSourceManifest,
    DatasetContract, FieldContract, PhysicalBindingContract, PolicyContract, compile_catalog,
    DriftAttestation,
)
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory
from research_pipeline.data_plane import (
    DateRangeV1, QueryIR, QueryPurpose, QueryBudget, SortKey, UniverseSelection,
    admit_query, build_logical_snapshot, publish_parquet_snapshot,
)
from research_pipeline.data_plane.dataset_artifacts import build_dataset_artifact_ref
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.platform import typed_canonical_hash
import synthetic


DATASET = "public.synthetic.etf.daily"
PRICE_FIELDS = ("fld_equity_daily_date", "fld_equity_daily_code", "fld_equity_daily_close")


def prepare_inputs(root, *, end_session=None):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    rows = synthetic.rows()
    if end_session is not None:
        rows = [row for row in rows if row[PRICE_FIELDS[0]] <= end_session]
    table = pa.Table.from_pylist(rows)
    policies = (
        PolicyContract("available.daily.v1", 1, "availability", {"available_after": "next_session_open"}),
        PolicyContract("drift.strict.v1", 1, "schema_drift", {"schema_changed": "reject"}),
        PolicyContract("revision.none.v1", 1, "revision", {"mode": "none"}),
    )
    fields = tuple(FieldContract(field.name, logical, 1, "date32" if pa.types.is_date32(field.type) else "float64" if pa.types.is_float64(field.type) else str(field.type), semantic, unit, False,
        "available.daily.v1", ("daily",), ("etf",), adjustment_allowed=("unadjusted",))
        for field, logical, semantic, unit in zip(table.schema,
        ("market.date", "instrument.id", "market.close", "market.open", "market.high_limit", "market.low_limit", "market.paused"),
        ("date", "identifier", "numeric", "numeric", "numeric", "numeric", "boolean"),
        ("day", "dimensionless", "CNY", "CNY", "CNY", "CNY", "dimensionless")))
    dataset = DatasetContract(DATASET, 1, "cn_etf", "etf", "daily", PRICE_FIELDS[:2],
        tuple(table.column_names), PRICE_FIELDS[0], "available.daily.v1", "drift.strict.v1", PRICE_FIELDS[:2])
    inventory = PhysicalInventory("parquet", "public_synthetic", "example", "snapshot",
        tuple(PhysicalColumn(field.name, str(field.type), field.nullable, i) for i, field in enumerate(table.schema)))
    binding = PhysicalBindingContract("synthetic.original", 1, DATASET, 1, "synthetic_generator", "example", "source",
        inventory.schema_revision, "drift.strict.v1", {field.field_id: {"kind": "direct", "column": field.field_id} for field in fields})
    archived = replace(binding, binding_id="synthetic.archived", source_profile="public_synthetic", object_name="snapshot")
    contracts = (*policies, *fields, dataset, binding, archived)
    kinds = {FieldContract: ("field", "field_id"), DatasetContract: ("dataset", "dataset_id"),
             PolicyContract: ("policy", "policy_id"), PhysicalBindingContract: ("physical_binding", "binding_id")}
    decisions = tuple(ApprovalDecision(kinds[type(item)][0], getattr(item, kinds[type(item)][1]), item.content_hash,
        "approved", "公开合成ETF输入", (), "example", "2026-10-03T00:00:00Z") for item in contracts)
    baseline = CatalogCoverageBaseline("public.synthetic.v1", 1, ("public_synthetic",))
    decisions += (ApprovalDecision("coverage_slot", "public_synthetic", None, "approved", "公开合成来源", (), "example", "2026-10-03T00:00:00Z"),)
    manifest = CatalogSourceManifest("public.synthetic.v1", 1, baseline.content_hash, None, ("public_synthetic",), (), {}, {"public_synthetic": (DATASET,)})
    catalog = compile_catalog(baseline=baseline, expected_baseline_hash=baseline.content_hash, manifest=manifest,
        contracts=contracts, decisions=decisions, release_root=root / "catalog")
    session_days = synthetic.sessions()
    outputs = {}
    requests = []
    for kind in ("feature", "label"):
        query = QueryIR(DATASET, 1, tuple(table.column_names), QueryPurpose(kind),
            DateRangeV1(session_days[0], end_session or session_days[99]), UniverseSelection(synthetic.instruments()), (),
            tuple(SortKey(name) for name in PRICE_FIELDS[:2]), QueryBudget(2000, 16*1024**2, 8192),
            adjustment="unadjusted", as_of=(end_session or session_days[-1]).isoformat())
        policy = policies[1]
        attestation = DriftAttestation(catalog.catalog_hash, binding.binding_id, binding.source_profile,
            binding.environment, binding.expected_schema_revision, binding.expected_schema_revision, policy.content_hash, True)
        plan = admit_query(query, catalog=catalog, binding=catalog.bindings[binding.binding_id], attestation=attestation)
        revision = SourceRevision("public_synthetic", "deterministic_generator",
            (SourceFileEvidence("synthetic.py", Path(synthetic.__file__).stat().st_size, 0),),
            typed_canonical_hash([{key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in row.items()} for row in synthetic.rows()]), None, "metadata")
        output = root / "inputs" / kind
        snapshot = publish_parquet_snapshot(batches=table.to_batches(), schema=table.schema, plan=plan,
            logical_snapshot=build_logical_snapshot(plan, source_revisions=(revision,), output_schema=table.schema,
                provider_version="public-synthetic-v1", compiler_version="public-synthetic-v1"), root=output)
        reference = build_dataset_artifact_ref(snapshot, allowed_root=output)
        outputs["daily_"+kind] = {"kind": "dataset", "original_plan": plan.to_dict(), "root": str(output),
            "reference": reference.to_dict(), "binding_id": archived.binding_id}
        request = query.to_dict()
        request.pop("contract_version", None)
        request["request_id"] = "daily_"+kind
        requests.append(request)
    manifest_path = root / "inputs.json"
    manifest_path.write_text(json.dumps({"contract_version": "archived-input-manifest-v1", "requests": outputs},
        ensure_ascii=False, indent=2), encoding="utf-8")
    return catalog, requests, manifest_path
