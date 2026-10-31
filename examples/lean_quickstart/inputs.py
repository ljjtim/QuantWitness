"""自编译公开Catalog与Parquet归档，不打开数据库。"""
from pathlib import Path
import json
from dataclasses import replace
from datetime import date, datetime
import pyarrow as pa
import pyarrow.parquet as pq
from research_pipeline.catalog import (ApprovalDecision, CatalogCoverageBaseline, CatalogSourceManifest,
    DatasetContract, FieldContract, PhysicalBindingContract, PolicyContract, compile_catalog, DriftAttestation)
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory
from research_pipeline.data_plane import (DateRangeV1, QueryIR, QueryPurpose, QueryBudget, SortKey,
    UniverseSelection, admit_query, build_logical_snapshot, publish_parquet_snapshot)
from research_pipeline.data_plane.dataset_artifacts import build_dataset_artifact_ref
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.platform import typed_canonical_hash
from lean_inputs import portfolio_sources
import synthetic


def prepare_inputs(root, frequency):
    extra, sources = portfolio_sources(synthetic.rows())
    policies = (PolicyContract("available.daily.v1",1,"availability",{"available_after":"next_session_open"}),
        PolicyContract("drift.strict.v1",1,"schema_drift",{"schema_changed":"reject"}),
        PolicyContract("revision.none.v1",1,"revision",{"mode":"none"}))
    contracts=[*policies,*extra]
    minute=None
    if frequency=="1m":
        from minute_inputs import minute_contracts
        additions, minute=minute_contracts(root)
        contracts.extend(additions)
    kinds={FieldContract:("field","field_id"),DatasetContract:("dataset","dataset_id"),
        PolicyContract:("policy","policy_id"),PhysicalBindingContract:("physical_binding","binding_id")}
    decisions=tuple(ApprovalDecision(kinds[type(item)][0],getattr(item,kinds[type(item)][1]),item.content_hash,
        "approved","公开确定性合成输入",(),"lean-quickstart","2024-01-12T01:00:00Z") for item in contracts)
    baseline=CatalogCoverageBaseline("public.lean.quickstart",1,("public_synthetic",))
    decisions+=(ApprovalDecision("coverage_slot","public_synthetic",None,"approved","合成来源",(),"lean-quickstart","2024-01-12T01:00:00Z"),)
    datasets=tuple(dict.fromkeys(item.dataset_id for item in contracts if isinstance(item,DatasetContract)))
    manifest=CatalogSourceManifest("public.lean.quickstart",1,baseline.content_hash,None,("public_synthetic",),(),{}, {"public_synthetic":datasets})
    catalog=compile_catalog(baseline=baseline,expected_baseline_hash=baseline.content_hash,manifest=manifest,
        contracts=tuple(contracts),decisions=decisions,release_root=root/"catalog")
    outputs={}; requests=[]
    for request_id,dataset,table,binding,archive in sources:
        start=synthetic.DAYS[0] if request_id=="portfolio_factors" else synthetic.DAYS[1]
        end=synthetic.DAYS[-1]
        query=QueryIR(dataset.dataset_id,1,tuple(table.column_names),QueryPurpose.FEATURE,
            DateRangeV1(date.fromisoformat(start),date.fromisoformat(end)),UniverseSelection((synthetic.CODE,)),(),
            tuple(SortKey(name) for name in dataset.sort_order),QueryBudget(2000,16*1024**2,8192),
            adjustment="unadjusted",as_of=synthetic.DAYS[-1])
        attestation=DriftAttestation(catalog.catalog_hash,binding.binding_id,binding.source_profile,binding.environment,
            binding.expected_schema_revision,binding.expected_schema_revision,policies[1].content_hash,True)
        plan=admit_query(query,catalog=catalog,binding=catalog.bindings[binding.binding_id],attestation=attestation)
        revision=SourceRevision("public_synthetic","deterministic_generator",
            (SourceFileEvidence("synthetic.py",Path(synthetic.__file__).stat().st_size,0),),
            typed_canonical_hash({"days":synthetic.DAYS,"prices":synthetic.PRICES}),None,"metadata")
        output=root/"inputs"/request_id
        snapshot=publish_parquet_snapshot(batches=table.to_batches(),schema=table.schema,plan=plan,
            logical_snapshot=build_logical_snapshot(plan,source_revisions=(revision,),output_schema=table.schema,
                provider_version="public-lean-v1",compiler_version="public-lean-v1"),root=output,
            temporal_source=plan.temporal_selection.requires_consumer_binding)
        reference=build_dataset_artifact_ref(snapshot,allowed_root=output)
        outputs[request_id]=dict(kind="dataset",original_plan=plan.to_dict(),root=str(output),reference=reference.to_dict(),binding_id=archive.binding_id)
        raw=query.to_dict(); raw.pop("contract_version",None); raw.pop("as_of",None); raw.pop("ir_version",None); raw["request_id"]=request_id; requests.append(raw)
    if minute:
        from minute_inputs import publish_minute
        raw,entry=publish_minute(root,catalog,minute,policies[1])
        requests.append(raw); outputs[raw["request_id"]]=entry
    target=root/"inputs.json"
    target.write_text(json.dumps(dict(contract_version="archived-input-manifest-v1",requests=outputs),ensure_ascii=False,indent=2),encoding="utf-8")
    return catalog,requests,target
