"""独立编译期货合成Catalog，并发布Parquet输入归档。"""
from dataclasses import replace
from datetime import date, datetime
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from research_pipeline.catalog import ApprovalDecision, CatalogCoverageBaseline, CatalogSourceManifest, DatasetContract, FieldContract, PhysicalBindingContract, PolicyContract, compile_catalog, DriftAttestation
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory
from research_pipeline.catalog.minute import MinuteDatasetSemantics, MinuteSourceSemantics
from research_pipeline.platform.minute_reference import load_minute_capability_manifest
from research_pipeline.data_plane import DateRangeV1, InstantRangeV2, QueryIR, QueryPurpose, QueryBudget, SortKey, UniverseSelection, admit_query, build_logical_snapshot, publish_parquet_snapshot, build_minute_scan_plan, build_minute_partitioned_dataset
from research_pipeline.data_plane.dataset_artifacts import build_dataset_artifact_ref
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.platform import typed_canonical_hash
import synthetic

DRIFT = PolicyContract('drift.strict.v1', 1, 'schema_drift', {'schema_changed': 'reject'})
MINUTE_AVAILABILITY = 'available.minute.completed_bar.v1'


def daily_contracts():
    contracts = [PolicyContract('available.daily.v1', 1, 'availability', {'available_after': 'next_session_open'}),
        PolicyContract('available.futures.open.v1', 1, 'availability', {'available_after': 'session_open',
            'available_time_field': 'fld_futures_open_date', 'timezone': 'Asia/Shanghai'})]
    sources = []
    for request_id, dataset_id, prefix, names, availability in (
        ('bars', 'cn_futures.daily_bar', 'fld_futures_daily_', ('date', 'code', 'open', 'high', 'low', 'close', 'volume'), 'available.daily.v1'),
        ('opening', 'cn_futures.session_open_state', 'fld_futures_open_', ('date', 'code', 'open'), 'available.futures.open.v1')):
        table = pa.Table.from_pylist([{prefix+name: row[name] for name in names} for row in synthetic.daily_rows()])
        fields = tuple(FieldContract(prefix+name, 'market.futures.'+request_id+'.'+name, 1,
            'date32' if name == 'date' else 'string' if name == 'code' else 'float64',
            'date' if name == 'date' else 'identifier' if name == 'code' else 'numeric',
            'day' if name == 'date' else 'dimensionless' if name == 'code' else 'contract' if name == 'volume' else 'CNY',
            False, availability, ('daily',), ('future_contract',), adjustment_allowed=('unadjusted',),
            observation_model='market_event' if request_id == 'opening' else 'static_reference',
            observation_keys={'event_time_field': prefix+'date', 'available_time_field': prefix+'date'} if request_id == 'opening' else {}) for name in names)
        keys = (prefix+'date', prefix+'code')
        dataset = DatasetContract(dataset_id, 1, 'cn_future', 'future_contract', 'daily', keys, tuple(table.column_names),
            prefix+'date', availability, DRIFT.policy_id, keys, revision_policy_id='revision.none.v1')
        physical = PhysicalInventory('parquet', 'public_synthetic', 'example', request_id,
            tuple(PhysicalColumn(name, str(field.type), field.nullable, index) for index, (name, field) in enumerate(zip(names, table.schema))))
        original = PhysicalBindingContract('synthetic.futures.'+request_id+'.original', 1, dataset_id, 1,
            'synthetic_generator', 'example', request_id, physical.schema_revision, DRIFT.policy_id,
            {prefix+name: {'kind': 'direct', 'column': name} for name in names})
        physical = PhysicalInventory('parquet', 'public_synthetic', 'example', request_id,
            tuple(PhysicalColumn(field.name, str(field.type), field.nullable, index) for index, field in enumerate(table.schema)))
        archived = replace(original, binding_id='synthetic.futures.'+request_id+'.archived', source_profile='public_synthetic',
            expected_schema_revision=physical.schema_revision,
            column_bindings={name: {'kind': 'direct', 'column': name} for name in table.column_names})
        contracts.extend((*fields, dataset, original, archived))
        sources.append((request_id, dataset, table, original, archived))
    return contracts, sources


def minute_contracts(root):
    identity = load_minute_capability_manifest().downstream_identity('catalog.minute.contracts')
    table = pa.Table.from_pylist(synthetic.minute_rows())
    directory = root/'minute/futures/year=2024/month=01'
    directory.mkdir(parents=True)
    pq.write_table(table, directory/'data.parquet')
    names = tuple(table.column_names)
    ids = tuple('fld_futures_minute_'+name for name in names)
    semantics = MinuteDatasetSemantics('cn_future', 'tradable', '1m', 'Asia/Shanghai', 'naive_local_wall_clock',
        'completed_bar_end', 'session.public.synthetic.future.v1', MINUTE_AVAILABILITY,
        'scope.public.synthetic.futures.v1', 'historical_batch', ('quality.minute.common_fields.v1',))
    source = MinuteSourceSemantics('raw', 'pit_allowed', 'collected', 'none', 'not_applicable', identity['binding_hash'], ('public.synthetic.futures',))
    fields = tuple(FieldContract(field_id, 'public.synthetic.futures.minute.'+field.name, 1,
        'timestamp[us]' if field.name == 'dt' else 'float64' if pa.types.is_float64(field.type) else str(field.type),
        'datetime' if field.name == 'dt' else 'identifier' if field.name == 'code' else 'numeric',
        'minute' if field.name == 'dt' else 'dimensionless' if field.name == 'code' else 'source_native_volume' if field.name == 'volume' else 'CNY',
        False, MINUTE_AVAILABILITY, ('minute',), ('future_contract',), adjustment_allowed=('raw',)) for field_id, field in zip(ids, table.schema))
    dataset = DatasetContract('cn_futures.minute_bar', 1, 'cn_future', 'future_contract', 'minute', ids[:2], ids, ids[0],
        MINUTE_AVAILABILITY, DRIFT.policy_id, ids[:2], minute_semantics=semantics.to_dict())
    physical = PhysicalInventory('parquet', 'public_synthetic', 'example', 'minute',
        tuple(PhysicalColumn(field.name, str(field.type), field.nullable, index) for index, field in enumerate(table.schema)))
    archived = PhysicalBindingContract('synthetic.futures.minute.archived', 1, dataset.dataset_id, 1,
        'public_synthetic', 'example', 'minute', physical.schema_revision, DRIFT.policy_id,
        {field_id: {'kind': 'direct', 'column': name} for field_id, name in zip(ids, names)}, minute_source_semantics=source.to_dict())
    original = replace(archived, binding_id='synthetic.futures.minute.original', source_profile='synthetic_generator')
    contracts = [*fields, dataset, original, archived,
        PolicyContract(MINUTE_AVAILABILITY, 1, 'availability', dict(market_visible_after='completed_bar_end',
            same_day_realtime=False, source_delivery='historical_batch', timezone='Asia/Shanghai')),
        PolicyContract('scope.public.synthetic.futures.v1', 1, 'reference_scope', identity,
            validator_id='catalog.minute.capability-manifest.v1', applicability={'frequency': 'minute', 'market': 'cn'})]
    return contracts, [('minute_bars', dataset, table, original, archived)]


def prepare_inputs(root, frequency):
    contracts, sources = daily_contracts() if frequency == '1d' else minute_contracts(root)
    contracts += [DRIFT, PolicyContract('revision.none.v1', 1, 'revision', {'mode': 'none'})]
    kinds = {FieldContract: ('field', 'field_id'), DatasetContract: ('dataset', 'dataset_id'),
             PolicyContract: ('policy', 'policy_id'), PhysicalBindingContract: ('physical_binding', 'binding_id')}
    decisions = tuple(ApprovalDecision(kinds[type(item)][0], getattr(item, kinds[type(item)][1]), item.content_hash,
        'approved', '公开合成期货输入', (), 'lean-futures', '2024-01-05T16:00:00Z') for item in contracts)
    decisions += (ApprovalDecision('coverage_slot', 'public_synthetic', None, 'approved', '合成来源', (), 'lean-futures', '2024-01-05T16:00:00Z'),)
    baseline = CatalogCoverageBaseline('public.lean.futures', 1, ('public_synthetic',))
    manifest = CatalogSourceManifest('public.lean.futures', 1, baseline.content_hash, None, ('public_synthetic',), (), {},
        {'public_synthetic': tuple(item.dataset_id for item in contracts if isinstance(item, DatasetContract))})
    catalog = compile_catalog(baseline=baseline, expected_baseline_hash=baseline.content_hash,
        manifest=manifest, contracts=tuple(contracts), decisions=decisions, release_root=root/'catalog')
    requests, outputs = [], {}
    for request_id, dataset, table, binding, archived in sources:
        minute = request_id == 'minute_bars'
        period = (InstantRangeV2(datetime.fromisoformat(synthetic.RUN_DAYS[0]+'T09:30:00+08:00'),
            datetime.fromisoformat(synthetic.RUN_DAYS[-1]+'T15:01:00+08:00'), 'Asia/Shanghai') if minute else
            DateRangeV1(date.fromisoformat(synthetic.RUN_DAYS[0]), date.fromisoformat(synthetic.RUN_DAYS[-1])))
        query = QueryIR(dataset.dataset_id, 1, dataset.fields, QueryPurpose.FEATURE, period,
            UniverseSelection(synthetic.CODES), (), tuple(SortKey(name) for name in dataset.sort_order),
            QueryBudget(5000, 16*1024**2, 4096), adjustment='raw' if minute else 'unadjusted',
            as_of=synthetic.AS_OF if minute else synthetic.AS_OF[:10], ir_version='query-ir-v2' if minute else 'query-ir-v1')
        attestation = DriftAttestation(catalog.catalog_hash, binding.binding_id, binding.source_profile, binding.environment,
            binding.expected_schema_revision, binding.expected_schema_revision, DRIFT.content_hash, True)
        plan = admit_query(query, catalog=catalog, binding=catalog.bindings[binding.binding_id], attestation=attestation)
        if minute:
            scan = build_minute_scan_plan(plan, source=root/'minute/futures', allowed_root=root/'minute')
            reference = build_minute_partitioned_dataset(scan)
            entry = dict(kind='minute', original_plan=plan.to_dict(), root=str(root/'minute'), reference=reference.to_dict(), binding_id=archived.binding_id)
        else:
            revision = SourceRevision('public_synthetic', 'deterministic_generator',
                (SourceFileEvidence('synthetic.py', Path(synthetic.__file__).stat().st_size, 0),),
                typed_canonical_hash({'days': synthetic.DAYS, 'codes': synthetic.CODES}), None, 'metadata')
            output = root/'inputs'/request_id
            snapshot = publish_parquet_snapshot(batches=table.to_batches(), schema=table.schema, plan=plan,
                logical_snapshot=build_logical_snapshot(plan, source_revisions=(revision,), output_schema=table.schema,
                    provider_version='public-futures-v1', compiler_version='public-futures-v1'), root=output,
                temporal_source=plan.temporal_selection.requires_consumer_binding)
            reference = build_dataset_artifact_ref(snapshot, allowed_root=output)
            entry = dict(kind='dataset', original_plan=plan.to_dict(), root=str(output), reference=reference.to_dict(), binding_id=archived.binding_id)
        raw = query.to_dict()
        for name in ('contract_version', 'as_of', 'ir_version'):
            raw.pop(name, None)
        raw['request_id'] = request_id
        requests.append(raw)
        outputs[request_id] = entry
    archive = root/'inputs.json'
    archive.write_text(json.dumps(dict(contract_version='archived-input-manifest-v1', requests=outputs), ensure_ascii=False, indent=2), encoding='utf-8')
    return requests, archive
