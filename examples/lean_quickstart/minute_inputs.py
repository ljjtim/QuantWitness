"""合成一分钟原始Parquet及正式归档身份。"""
from dataclasses import replace
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from research_pipeline.catalog import DatasetContract,FieldContract,PhysicalBindingContract,PolicyContract,DriftAttestation
from research_pipeline.catalog.discovery import PhysicalInventory,PhysicalColumn
from research_pipeline.catalog.minute import MinuteDatasetSemantics,MinuteSourceSemantics
from research_pipeline.platform.minute_reference import load_minute_capability_manifest
from research_pipeline.data_plane import QueryIR,QueryPurpose,QueryBudget,SortKey,UniverseSelection,InstantRangeV2,admit_query,build_minute_scan_plan,build_minute_partitioned_dataset
import synthetic
FIELDS=tuple("fld_demo_minute_"+name for name in ("dt","code","open","high","low","close","volume","money","avg"))
AVAILABILITY="available.minute.completed_bar.v1"


def minute_contracts(root):
    capability=load_minute_capability_manifest()
    identity=capability.downstream_identity("catalog.minute.contracts")
    semantics=MinuteDatasetSemantics("cn_etf","tradable","1m","Asia/Shanghai","naive_local_wall_clock","completed_bar_end",
        "session.public.synthetic.etf.v1",AVAILABILITY,"scope.public.synthetic.minute.v1","historical_batch",
        ("quality.minute.common_fields.v1","quality.minute.fund.v1"))
    source=MinuteSourceSemantics("raw","pit_allowed","collected","none","not_applicable",identity["binding_hash"],("public.synthetic.generator",))
    table=pa.Table.from_pylist(synthetic.minute_rows())
    directory=root/"minute"/"fund"/"year=2024"/"month=01"; directory.mkdir(parents=True)
    pq.write_table(table,directory/"data.parquet")
    inventory=PhysicalInventory("parquet","public_synthetic","example","minute",
        tuple(PhysicalColumn(f.name,str(f.type),f.nullable,index) for index,f in enumerate(table.schema)))
    fields=[]
    for field_id,f in zip(FIELDS,table.schema):
        semantic="datetime" if f.name=="dt" else "identifier" if f.name=="code" else "numeric"
        unit="minute" if f.name=="dt" else "dimensionless" if f.name=="code" else "source_native_volume" if f.name=="volume" else "CNY"
        fields.append(FieldContract(field_id,"public.synthetic.minute."+f.name,1,
            "timestamp[us]" if f.name=="dt" else "float64" if pa.types.is_float64(f.type) else str(f.type),semantic,unit,False,AVAILABILITY,("minute",),("etf",),adjustment_allowed=("raw",)))
    dataset=DatasetContract("cn_fund.minute_bar",1,"cn_etf","etf","minute",FIELDS[:2],FIELDS,FIELDS[0],AVAILABILITY,
        "drift.strict.v1",FIELDS[:2],minute_semantics=semantics.to_dict())
    binding=PhysicalBindingContract("synthetic.minute.archived",1,dataset.dataset_id,1,"public_synthetic","example","minute",
        inventory.schema_revision,"drift.strict.v1",{name:{"kind":"direct","column":field.name} for name,field in zip(FIELDS,table.schema)},
        minute_source_semantics=source.to_dict())
    policies=[PolicyContract(AVAILABILITY,1,"availability",dict(market_visible_after="completed_bar_end",same_day_realtime=False,source_delivery="historical_batch",timezone="Asia/Shanghai")),
        PolicyContract("scope.public.synthetic.minute.v1",1,"reference_scope",identity,validator_id="catalog.minute.capability-manifest.v1",applicability={"frequency":"minute","market":"cn"})]
    original=replace(binding,binding_id="synthetic.minute.original",source_profile="synthetic_generator")
    return (*policies,*fields,dataset,original,binding),(dataset,original,binding)


def publish_minute(root,catalog,source,drift):
    dataset,binding,archived=source
    query=QueryIR(dataset.dataset_id,1,FIELDS,QueryPurpose.FEATURE,
        InstantRangeV2(datetime.fromisoformat(synthetic.DAYS[1]+"T09:30:00+08:00"),datetime.fromisoformat(synthetic.DAYS[-1]+"T15:01:00+08:00"),"Asia/Shanghai"),
        UniverseSelection((synthetic.CODE,)),(),tuple(SortKey(name) for name in FIELDS[:2]),QueryBudget(2000,16*1024**2,1024),adjustment="raw",as_of="2024-01-11T23:59:59+08:00",ir_version="query-ir-v2")
    attestation=DriftAttestation(catalog.catalog_hash,binding.binding_id,binding.source_profile,binding.environment,
        binding.expected_schema_revision,binding.expected_schema_revision,drift.content_hash,True)
    plan=admit_query(query,catalog=catalog,binding=catalog.bindings[binding.binding_id],attestation=attestation)
    minute_root=root/"minute"
    scan=build_minute_scan_plan(plan,source=minute_root/"fund",allowed_root=minute_root)
    reference=build_minute_partitioned_dataset(scan)
    raw=query.to_dict(); raw.pop("contract_version",None); raw.pop("as_of",None); raw.pop("ir_version",None); raw["request_id"]="minute_bars"
    return raw,dict(kind="minute",original_plan=plan.to_dict(),root=str(minute_root),reference=reference.to_dict(),binding_id=archived.binding_id)


def scan_parameters():
    return dict(request_ids=["minute_bars"],scope_binding_hash=load_minute_capability_manifest().manifest_hash,
        max_source_bytes=16*1024**2,max_returned_rows=2000,max_batch_bytes=16*1024**2,availability_policy_ref=AVAILABILITY)
