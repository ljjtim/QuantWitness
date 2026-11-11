"""生成三个合成观察日的Catalog与已准入Parquet归档。"""
from dataclasses import replace
from datetime import date
import json
from pathlib import Path
import pyarrow as pa
from research_pipeline.catalog import (
    ApprovalDecision, CatalogCoverageBaseline, CatalogSourceManifest, DatasetContract,
    FieldContract, PhysicalBindingContract, PolicyContract, compile_catalog, DriftAttestation,
)
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory
from research_pipeline.data_plane import (
    DateRangeV1, QueryIR, QueryPurpose, QueryBudget, SortKey, UniverseSelection,
    admit_query, build_logical_snapshot, publish_parquet_snapshot,
)
from research_pipeline.data_plane.dataset_artifacts import build_dataset_artifact_ref
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.platform import typed_canonical_hash


def prepare_inputs(root: Path):
    table = pa.table({'fld_metric_session': pa.array([date(2024, 1, day) for day in (3, 4, 5)], type=pa.date32()),
                      'fld_metric_code': ['SYN.OBS'] * 3,
                      'fld_metric_return': pa.array([0.01, -0.02, 0.04], type=pa.float64())})
    policies = (
        PolicyContract('custom.available', 1, 'availability', {'available_after': 'next_session_open'}),
        PolicyContract('custom.drift', 1, 'schema_drift', {'schema_changed': 'reject'}),
        PolicyContract('revision.none.v1', 1, 'revision', {'mode': 'none'}),
    )
    roles = [('date32', 'date', 'day'), ('string', 'identifier', 'dimensionless'), ('float64', 'numeric', 'decimal_return')]
    fields = tuple(FieldContract(field.name, 'custom.' + field.name.removeprefix('fld_metric_'), 1,
                   dtype, semantic, unit, False, 'custom.available', ('daily',), ('etf',),
                   adjustment_allowed=('unadjusted',)) for field, (dtype, semantic, unit) in zip(table.schema, roles))
    dataset = DatasetContract('custom.synthetic.observations', 1, 'cn_etf', 'etf', 'daily',
        ('fld_metric_session', 'fld_metric_code'), tuple(table.column_names), 'fld_metric_session',
        'custom.available', 'custom.drift', ('fld_metric_session', 'fld_metric_code'))
    inventory = PhysicalInventory('parquet', 'custom_synthetic', 'example', 'snapshot',
        tuple(PhysicalColumn(field.name, str(field.type), field.nullable, i) for i, field in enumerate(table.schema)))
    binding = PhysicalBindingContract('custom.original', 1, dataset.dataset_id, 1, 'custom_generator', 'example', 'source',
        inventory.schema_revision, 'custom.drift', {field.field_id: {'kind': 'direct', 'column': field.field_id} for field in fields})
    archived = replace(binding, binding_id='custom.archived', source_profile='custom_synthetic', object_name='snapshot')
    contracts = (*policies, *fields, dataset, binding, archived)
    kinds = {FieldContract: ('field', 'field_id'), DatasetContract: ('dataset', 'dataset_id'),
             PolicyContract: ('policy', 'policy_id'), PhysicalBindingContract: ('physical_binding', 'binding_id')}
    decisions = tuple(ApprovalDecision(kinds[type(item)][0], getattr(item, kinds[type(item)][1]), item.content_hash,
        'approved', '确定性合成观察，只用于指标接入', (), 'example', '2024-01-09T00:00:00Z') for item in contracts)
    decisions += (ApprovalDecision('coverage_slot', 'custom_synthetic', None, 'approved', '公开合成来源', (), 'example', '2024-01-09T00:00:00Z'),)
    baseline = CatalogCoverageBaseline('custom.synthetic', 1, ('custom_synthetic',))
    manifest = CatalogSourceManifest('custom.synthetic', 1, baseline.content_hash, None, ('custom_synthetic',), (), {},
                                     {'custom_synthetic': (dataset.dataset_id,)})
    catalog = compile_catalog(baseline=baseline, expected_baseline_hash=baseline.content_hash, manifest=manifest,
        contracts=contracts, decisions=decisions, release_root=root / 'catalog')
    query = QueryIR(dataset.dataset_id, 1, tuple(table.column_names), QueryPurpose.FEATURE,
        DateRangeV1(date(2024, 1, 3), date(2024, 1, 5)), UniverseSelection(('SYN.OBS',)), (),
        tuple(SortKey(name) for name in dataset.sort_order), QueryBudget(16, 1048576, 16),
        adjustment='unadjusted', as_of='2024-01-08')
    attestation = DriftAttestation(catalog.catalog_hash, binding.binding_id, binding.source_profile,
        binding.environment, binding.expected_schema_revision, binding.expected_schema_revision, policies[1].content_hash, True)
    plan = admit_query(query, catalog=catalog, binding=catalog.bindings[binding.binding_id], attestation=attestation)
    source = Path(__file__)
    revision = SourceRevision('custom_synthetic', 'deterministic_generator',
        (SourceFileEvidence(source.name, source.stat().st_size, 0),),
        typed_canonical_hash({'dates': ['2024-01-03', '2024-01-04', '2024-01-05'], 'returns': [0.01, -0.02, 0.04]}), None, 'metadata')
    archive_root = root / 'inputs'
    snapshot = publish_parquet_snapshot(batches=table.to_batches(), schema=table.schema, plan=plan,
        logical_snapshot=build_logical_snapshot(plan, source_revisions=(revision,), output_schema=table.schema,
        provider_version='custom-synthetic-v1', compiler_version='custom-synthetic-v1'), root=archive_root,
        temporal_source=plan.temporal_selection.requires_consumer_binding)
    reference = build_dataset_artifact_ref(snapshot, allowed_root=archive_root)
    archive = root / 'inputs.json'
    archive.write_text(json.dumps({'contract_version': 'archived-input-manifest-v1', 'requests': {
        'observations': {'kind': 'dataset', 'original_plan': plan.to_dict(), 'root': str(archive_root),
                         'reference': reference.to_dict(), 'binding_id': archived.binding_id}}}, ensure_ascii=False, indent=2), encoding='utf-8')
    request = query.to_dict()
    request.pop('ir_version')
    request['request_id'] = 'observations'
    return request, archive
