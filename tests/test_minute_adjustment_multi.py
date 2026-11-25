"""批量分钟复权的逐证券隔离、时点选择及正式工件交接。"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from research_pipeline.catalog import AdjustmentFactorSnapshot, CatalogReferenceError, CompiledCatalog, DriftAttestation, PolicyContract
from research_pipeline.data_plane import DatasetFilter, DateRangeV1, PartitionedDatasetRef, QueryBudget, QueryIR, QueryPurpose, SortKey, UniverseSelection, admit_query, build_dataset_artifact_ref, build_logical_snapshot, inspect_parquet_partition, publish_parquet_snapshot
from research_pipeline.data_plane.errors import QualityGateError
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.runtime import ExternalArtifactStore, RuntimeNodeValue
from research_pipeline.runtime.adapters import minute_adjustment as adapter
from research_pipeline.runtime.adapters.common import ResearchRunEnvironment, _external_result
from research_pipeline.runtime.adapters.minute_strategy_pit import strategy_snapshot_context
from research_pipeline.runtime.lean_strategy_inputs import _actions as lean_actions
TZ = ZoneInfo('Asia/Shanghai')
CODES = ('000001.XSHE', '600000.XSHG')
START = datetime(2024, 1, 2, 9, 31, tzinfo=TZ)
END = datetime(2024, 1, 5, 15, 1, tzinfo=TZ)
DAILY_ID = 'stock_daily_factor'
ACTION_ID = 'stock_corporate_actions'
CALENDAR_ID = 'corporate_action_calendar'
OPEN_ID = 'stock_session_open'
SNAPSHOT_TYPE = 'data.adjustment-factor-snapshot.v1'

def _action(code, action_id, *, cash='1.0', revised=None):
    return {'fld_ca_id': action_id, 'fld_ca_code': code, 'fld_ca_implementation_pub_date': date(2024, 1, 2), 'fld_ca_cash_per_ten': Decimal(cash), 'fld_ca_stock_per_ten': Decimal(0), 'fld_ca_transfer_per_ten': Decimal(0), 'fld_ca_record_date': date(2024, 1, 3), 'fld_ca_ex_date': date(2024, 1, 4), 'fld_ca_cash_arrival_date': date(2024, 1, 5), 'fld_ca_stock_arrival_date': None, 'fld_ca_transfer_arrival_date': None, 'fld_ca_listing_date': None, 'fld_ca_plan_progress': '实施方案', 'fld_ca_status': 0, 'fld_ca_note': None, 'fld_ca_add_time': datetime(2024, 1, 2, 8), 'fld_ca_mod_time': revised or datetime(2024, 1, 2, 8)}

def _daily(codes=CODES):
    return [{'fld_equity_daily_date': day, 'fld_equity_daily_code': code, 'fld_equity_daily_close': float(10 if index == 0 else 20), 'fld_equity_daily_factor': float(2 if index == 0 else 3)} for day in (date(2023, 12, 29), *(date(2024, 1, day) for day in (2, 3, 4, 5))) for (index, code) in enumerate(codes)]

def _parameters(codes=CODES):
    return {'factor_request_id': DAILY_ID, 'corporate_action_request_id': ACTION_ID, 'instrument_ids': list(codes), 'asset_class': 'cn_stock', 'as_of': END.isoformat(), 'applicable_start': START.isoformat(), 'applicable_end': END.isoformat()}

def _context(store, plans, parameters, *, inputs=None, port='snapshot', artifact_type=SNAPSHOT_TYPE):
    environment = ResearchRunEnvironment(plan_root=store.root, manifest={}, admitted_plans=plans, database=None, source_databases={}, minute_data_root=None, holdout_ledger_anchor=store.root, node_parameters={'node': parameters}, semantics=None, fixed_clock=END.isoformat(), root_seed=1, workers=1, resource_timeout_seconds=None, strategy_spec_hashes={})
    return SimpleNamespace(environment=environment, node=SimpleNamespace(node_id='node', output_types=((port, artifact_type),)), inputs=inputs or {}, external_store=store, effective_resource_budget=SimpleNamespace(memory_bytes=256 * 1024 * 1024))

def _plan(request, codes):
    catalog = CompiledCatalog.load_default()
    dataset_id = request['dataset_id']
    binding = next((item for item in catalog.bindings.values() if item['dataset_id'] == dataset_id))
    policy = PolicyContract(**dict(catalog.policies[binding['drift_policy_id']]))
    attestation = DriftAttestation(catalog.catalog_hash, binding['binding_id'], binding['source_profile'], binding['environment'], binding['expected_schema_revision'], binding['expected_schema_revision'], policy.content_hash, True)
    is_calendar = dataset_id == 'cn_market.trading_sessions'
    universe = UniverseSelection(snapshot_id='cn_market_trading_sessions') if is_calendar else UniverseSelection(tuple(codes))
    purpose = QueryPurpose.UNIVERSE if is_calendar else QueryPurpose.FEATURE
    query = QueryIR(dataset_id, int(catalog.datasets[dataset_id]['dataset_version']), tuple(request['field_ids']), purpose, DateRangeV1(date(2023, 12, 29), date(2024, 1, 5)), universe, (), tuple((SortKey(item['field_id']) for item in request['sort'])), QueryBudget(100000, 64 * 1024 * 1024, 8192), adjustment=request['adjustment'])
    return admit_query(query, catalog=catalog, binding=binding, attestation=attestation)

def _case(tmp_path, *, codes=CODES, daily=None, actions=None, calendar=None, opening=None):
    requests = yaml.safe_load((Path(__file__).resolve().parents[1] / 'research_packages/references/minute_stock_session/spec/research.yaml').read_text(encoding='utf-8'))['requests'][:2]
    plans = {request['request_id']: _plan(request, codes) for request in requests}
    rows_by_request = {DAILY_ID: _daily(codes) if daily is None else daily, ACTION_ID: [_action(codes[0], 1), _action(codes[-1], 2, cash='4.0')] if actions is None else actions}
    if calendar is not None:
        field = 'fld_cn_trade_day'
        plans[CALENDAR_ID] = _plan({
            'dataset_id': 'cn_market.trading_sessions', 'field_ids': [field],
            'sort': [{'field_id': field}], 'adjustment': 'unadjusted',
        }, ())
        rows_by_request[CALENDAR_ID] = [{field: day} for day in calendar]
    if opening is not None:
        plans[OPEN_ID] = _plan({
            'dataset_id': 'cn_equity.session_open_state', 'field_ids': adapter._STOCK_OPEN_FIELDS,
            'sort': [{'field_id': name} for name in adapter._STOCK_OPEN_FIELDS[:2]],
            'adjustment': 'unadjusted',
        }, codes)
        rows_by_request[OPEN_ID] = opening
    store = ExternalArtifactStore(tmp_path / 'store')
    source = tmp_path / 'source'
    references = {}
    types = {'date32': pa.date32(), 'decimal128': pa.decimal128(38, 18), 'float64': pa.float64(), 'int64': pa.int64(), 'int8': pa.int8(), 'int16': pa.int16(), 'string': pa.string(), 'timestamp': pa.timestamp('us')}
    for (request_id, plan) in plans.items():
        temporal = request_id == ACTION_ID
        fields = plan.temporal_selection.required_scan_fields if temporal else plan.query.field_ids
        schema = pa.schema([(field, types[dict(plan.field_types)[field]]) for field in fields])
        rows = rows_by_request[request_id]
        rows = sorted(rows, key=lambda row: tuple((row[field] for field in plan.primary_key)))
        table = pa.Table.from_pylist(rows, schema=schema)
        revision = SourceRevision('fixture', 'parquet', (SourceFileEvidence(f'{request_id}.arrow', 10, 20),), 'schema', '2024-01-05', 'metadata')
        logical = build_logical_snapshot(plan, source_revisions=(revision,), output_schema=schema, provider_version='fixture-v1', compiler_version='fixture-v1')
        snapshot = publish_parquet_snapshot(batches=table.to_batches(max_chunksize=2), schema=schema, plan=plan, logical_snapshot=logical, root=source, temporal_source=temporal)
        references[request_id] = build_dataset_artifact_ref(snapshot, allowed_root=source).to_dict()
    data_context = _context(store, plans, {}, port='data', artifact_type='data.columnar-bundle.v1')
    data = _external_result(data_context, {'data_bundle': {'references': references}}, directories={'data': source}).values['data']
    parameters = _parameters(codes)
    if calendar is not None:
        parameters['trading_calendar_request_id'] = CALENDAR_ID
    if opening is not None:
        parameters['stock_open_state_request_id'] = OPEN_ID
    context = _context(store, plans, parameters, inputs={'data': data})
    return SimpleNamespace(store=store, plans=plans, parameters=parameters, context=context, references=references)

def _read(case, outputs, port='snapshot'):
    value = outputs.values[port]
    commit = case.store.verify(value.external_commit.semantic_hash)
    root = case.store.objects_root / commit.semantic_hash
    return (json.loads((root / 'result.json').read_text(encoding='utf-8')), root, value)

def _snapshot(case):
    return _read(case, adapter.execute_data_minute_adjustment_snapshot_v1(case.context))


def test_raw_stock_actions_match_financial_snapshot_across_start_boundary(tmp_path):
    calendar = (date(2023, 12, 29),
                *(date(2024, 1, day) for day in (2, 3, 4, 5)))
    winter = {
        **_action(CODES[0], 1),
        'fld_ca_implementation_pub_date': date(2023, 12, 30),
        'fld_ca_record_date': date(2024, 1, 2),
    }
    case = _case(tmp_path, actions=[winter, _action(CODES[1], 2)], calendar=calendar)
    snapshot = _snapshot(case)[0]['snapshots']
    actual = adapter.compile_stock_action_facts(
        case.context, action_request_id=ACTION_ID, calendar_request_id=CALENDAR_ID,
        instrument_ids=CODES, applicable_start=datetime(2024, 1, 3, 9, 30, tzinfo=TZ),
        applicable_end=END, as_of=END,
    )
    assert {code: [action.to_dict() for action in actions] for code, actions in actual.items()} == {
        code: member['financial_corporate_actions'] for code, member in snapshot.items()
    }
    assert actual[CODES[0]][0].record_date < date(2024, 1, 3)


def test_raw_stock_actions_keep_visible_revisions_and_pending_registration(tmp_path):
    calendar = (date(2023, 12, 29), *(date(2024, 1, day) for day in (2, 3, 4, 5)))
    first = {**_action(CODES[0], 1), 'fld_ca_implementation_pub_date': date(2023, 12, 29)}
    amended = {
        **first,
        'fld_ca_implementation_pub_date': date(2024, 1, 3),
        'fld_ca_cash_per_ten': Decimal('2.0'),
    }
    pending = {
        **_action(CODES[1], 2),
        'fld_ca_ex_date': date(2024, 1, 5),
    }
    case = _case(tmp_path, actions=[first, amended, pending], calendar=calendar)
    kwargs = {
        'action_request_id': ACTION_ID, 'calendar_request_id': CALENDAR_ID,
        'instrument_ids': CODES, 'applicable_start': START,
        'applicable_end': datetime(2024, 1, 4, 15, 1, tzinfo=TZ), 'as_of': END,
    }
    actual = adapter.compile_stock_action_facts(case.context, **kwargs)
    assert [(action.revision, action.cash_per_share_microunits) for action in actual[CODES[0]]] == [
        (1, 100_000), (2, 200_000),
    ]
    assert [action.effective_date for action in actual[CODES[1]]] == [date(2024, 1, 5)]

    kwargs['applicable_end'] = datetime(2024, 1, 4, 9, 30, tzinfo=TZ)
    adjustments = adapter.compile_stock_action_facts(case.context, **kwargs, contract_version=1)
    assert adjustments == dict.fromkeys(CODES, ())


@pytest.mark.parametrize(
    ("operator", "value", "expected"),
    [("eq", "1", [1]), ("in", ("1", "2"), [1, 2])],
)
def test_archived_stock_action_ids_accept_integer_text(tmp_path, operator, value, expected):
    case = _case(tmp_path)
    dataset, _, plan = adapter._columnar_request_dataset(case.context, ACTION_ID)
    field = "fld_ca_id"
    filters = (DatasetFilter(field, operator, value),)
    batches = dataset.iter_batches_at(
        plan=plan, consumer_time=END, columns=(field,), filters=filters, batch_size=8192,
    )
    assert [row[field] for batch in batches for row in batch.to_pylist()] == expected
    with pytest.raises(pa.ArrowInvalid):
        tuple(dataset.iter_batches_at(
            plan=plan, consumer_time=END, columns=(field,),
            filters=(DatasetFilter(field, operator, "1.25" if operator == "eq" else ("1.25",)),),
            batch_size=8192,
        ))


def _opening_rows(codes=CODES):
    rows = []
    for index, code in enumerate(codes):
        for day in range(2, 6):
            factor = (2.0 if day < 4 else 2.0777123456789) if index == 0 else (3.0 if day < 3 else 3.1)
            pre_close = 9.95 if index == 0 and day == 4 else 20.0
            rows.append(dict(zip(adapter._STOCK_OPEN_FIELDS, (
                date(2024, 1, day), code, factor, pre_close,
            ))))
    return rows


@pytest.mark.parametrize('single', [False, True])
def test_open_stock_factor_and_market_reference_keep_financial_cash(tmp_path, single):
    codes = CODES[:1] if single else CODES
    calendar = (date(2023, 12, 29), *(date(2024, 1, day) for day in range(2, 6)))
    case = _case(tmp_path, codes=codes, actions=[_action(CODES[0], 1)],
                 calendar=calendar, opening=_opening_rows(codes))
    if single:
        case.parameters.pop('instrument_ids')
        case.parameters['instrument_id'] = CODES[0]
    result, _, _ = _snapshot(case)
    member = result if single else result['snapshots'][CODES[0]]
    assert member['input_references'][OPEN_ID] == case.references[OPEN_ID]
    snapshot = AdjustmentFactorSnapshot.from_mapping(member['snapshot'])
    assert snapshot.factor_source == 'stock_session_open'
    before = datetime(2024, 1, 4, 9, 29, 59, tzinfo=TZ)
    at_open = datetime(2024, 1, 4, 9, 30, tzinfo=TZ)
    assert Decimal(snapshot.segment_at(before).factor) == Decimal(2)
    segment = snapshot.segment_at(at_open)
    assert segment.starts_at == segment.available_at == at_open.isoformat()
    assert segment.factor == '2.0777123456789'
    assert segment.ex_reference_price == '9.95'
    assert float(member['financial_corporate_actions'][0]['cash_per_share_microunits']) == 100000
    contexts = strategy_snapshot_context(result, source_bars_hash='raw')
    events, _ = lean_actions(contexts, codes, ('2024-01-02', '2024-01-05'), lambda code, day: 995)
    assert [(item['cash_per_share'], item['ex_reference_price']) for item in events] == [(0.1, 9.95)]
    if not single:
        other = AdjustmentFactorSnapshot.from_mapping(result['snapshots'][CODES[1]]['snapshot'])
        at_change = datetime(2024, 1, 3, 9, 30, tzinfo=TZ)
        segment = other.segment_at(at_change)
        assert (segment.factor, segment.ex_reference_price, segment.action_hashes) == ('3.1', '20.0', ())


def test_open_stock_factor_requires_coverage_and_uses_real_ipo_factor(tmp_path):
    calendar = (date(2023, 12, 29), *(date(2024, 1, day) for day in range(2, 6)))
    code = CODES[0]
    rows = _opening_rows((code,))
    first_day = date(2024, 1, 2)
    rows[0][adapter._STOCK_OPEN_FIELDS[2]] = 1.03
    rows[0][adapter._STOCK_OPEN_FIELDS[3]] = None
    daily = [row for row in _daily((code,)) if row['fld_equity_daily_date'] >= first_day]
    complete = _case(tmp_path / 'complete', codes=(code,), actions=[], calendar=calendar, opening=rows, daily=daily)
    evidence = {'instrument_id': code, 'listed_date': str(first_day),
                'available_at': '2023-12-29T17:00:00+08:00', 'source_reference': 'ipo'}
    complete.parameters['initial_listing_evidence_by_instrument'] = {code: evidence}
    member = _snapshot(complete)[0]['snapshots'][code]
    assert member['snapshot']['initial_factor'] == member['snapshot']['factor_segments'][0]['factor'] == '1.03'
    assert member['snapshot']['factor_segments'][0]['ex_reference_price'] is None
    complete.parameters['applicable_start'] = '2024-01-02T09:29:00+08:00'
    with pytest.raises(ValueError, match='上市首日适用起点不能早于'):
        _snapshot(complete)
    incomplete = _case(tmp_path / 'missing', codes=(code,), actions=[], calendar=calendar, opening=rows[:2] + rows[3:], daily=daily)
    incomplete.parameters['initial_listing_evidence_by_instrument'] = {code: evidence}
    with pytest.raises(ValueError, match='股票开盘因子交易日覆盖不完整'):
        _snapshot(incomplete)


def test_open_stock_factor_does_not_extend_into_missing_tail(tmp_path):
    calendar = (date(2023, 12, 29), *(date(2024, 1, day) for day in range(2, 6)))
    code = CODES[0]
    last_day = date(2024, 1, 4)
    rows = [row for row in _opening_rows((code,)) if row[adapter._STOCK_OPEN_FIELDS[0]] <= last_day]
    daily = [row for row in _daily((code,)) if row['fld_equity_daily_date'] <= last_day]
    case = _case(tmp_path, codes=(code,), actions=[], calendar=calendar, opening=rows, daily=daily)
    snapshot = AdjustmentFactorSnapshot.from_mapping(_snapshot(case)[0]['snapshots'][code]['snapshot'])
    assert snapshot.applicable_end == '2024-01-05T09:30:00+08:00'
    with pytest.raises(CatalogReferenceError, match='未覆盖消费时点'):
        snapshot.segment_at(datetime.fromisoformat(snapshot.applicable_end))


@pytest.mark.parametrize('single', [False, True])
def test_frozen_market_calendar_covers_announcement_before_factor_window(tmp_path, single):
    publication = date(2023, 12, 29)
    first_after_holiday = date(2024, 1, 2)
    calendar = (publication, *(date(2024, 1, day) for day in (2, 3, 4, 5)))
    action = {**_action(CODES[0], 1), 'fld_ca_implementation_pub_date': publication}
    daily = [row for row in _daily() if row['fld_equity_daily_date'] != first_after_holiday]
    case = _case(tmp_path, daily=daily, actions=[action], calendar=calendar)
    if single:
        case.parameters.pop('instrument_ids')
        case.parameters['instrument_id'] = CODES[0]
    result = _snapshot(case)[0]
    payload = result if single else result['snapshots'][CODES[0]]
    for key in ('candidate_actions', 'financial_corporate_actions'):
        assert [row['announcement_available_time'] for row in payload[key]] == ['2024-01-02T09:15:00+08:00']
    assert payload['input_references'][CALENDAR_ID] == case.references[CALENDAR_ID]
    case.parameters.pop('trading_calendar_request_id')
    with pytest.raises(ValueError, match='缺少完整市场日历'):
        _snapshot(case)


def test_financial_null_date_policy_keeps_same_pit_snapshot_and_other_cash(tmp_path):
    actions = [_action(CODES[0], 1)]
    actions[0].update(fld_ca_transfer_per_ten=Decimal('3'),
                      fld_ca_transfer_arrival_date=None, fld_ca_listing_date=date(2024, 1, 5))
    omitted = _case(tmp_path / 'omitted', codes=(CODES[0],), actions=actions)
    omitted.parameters['financial_missing_date_policy'] = 'omit_event'
    outputs = adapter.execute_data_minute_adjustment_snapshot_v1(omitted.context)
    assert 'omit_event' in outputs.completion_metadata.limitations[0]
    payload = _read(omitted, outputs)[0]['snapshots'][CODES[0]]
    assert [row['action_id'] for row in payload['financial_corporate_actions']] == ['1:cash']
    assert any(row['kind'] == 'stock_dividend' for row in payload['included_actions'])
    complete_actions = deepcopy(actions)
    complete_actions[0]['fld_ca_transfer_arrival_date'] = date(2024, 1, 5)
    complete = _case(tmp_path / 'complete', codes=(CODES[0],), actions=complete_actions)
    baseline = _snapshot(complete)[0]['snapshots'][CODES[0]]
    assert payload['snapshot']['factor_segments'] == baseline['snapshot']['factor_segments']
    assert payload['included_actions'] == baseline['included_actions']
    strict = _case(tmp_path / 'strict', codes=(CODES[0],), actions=actions)
    with pytest.raises(ValueError, match='transfer_arrival_date'):
        _snapshot(strict)


def test_receivable_policy_is_frozen_in_financial_snapshot_and_limitations(tmp_path):
    actions = [_action(CODES[0], 1)]
    actions[0].update(fld_ca_transfer_per_ten=Decimal('3'),
                      fld_ca_transfer_arrival_date=None, fld_ca_listing_date=date(2024, 1, 5))
    case = _case(tmp_path, codes=(CODES[0],), actions=actions)
    case.parameters['financial_missing_date_policy'] = 'sellable_date_receivable'
    outputs = adapter.execute_data_minute_adjustment_snapshot_v1(case.context)
    assert 'sellable_date_receivable' in outputs.completion_metadata.limitations[0]
    payload = _read(case, outputs)[0]['snapshots'][CODES[0]]
    transfer = next(row for row in payload['financial_corporate_actions'] if row['action_id']=='1:transfer')
    assert transfer['shares_arrival_date'] is None
    assert transfer['shares_sellable_date']==transfer['pay_date']=='2024-01-05'
    assert transfer['share_settlement_policy']=='sellable_date_receivable'
    assert any(row['action_id']=='1:cash' for row in payload['financial_corporate_actions'])


def test_account_entitlement_exclusion_preserves_source_adjustment(tmp_path):
    code = "999999.XSHE"
    actions = [_action(code, 1)]
    actions[0].update(fld_ca_transfer_per_ten=Decimal("3"),
                      fld_ca_listing_date=date(2024, 1, 5))
    case = _case(tmp_path, codes=(code,), actions=actions)
    case.parameters["financial_missing_date_policy"] = "sellable_date_receivable"
    baseline = _snapshot(case)[0]["snapshots"][code]
    exclusion = {
        "action_id": "1", "instrument_id": code, "component": "transfer",
        "announcement_date": "2024-01-02", "record_date": "2024-01-03", "ex_date": "2024-01-04",
        "source_ratio_per_ten": "3", "source_dates": {
            "transfer_arrival_date": None, "listing_date": "2024-01-05",
        }, "source_note": None, "disposition": "omit_unresolved", "evidence_ref": "local/decisions.json#1",
    }
    case.parameters["financial_entitlement_exclusions"] = [exclusion]
    outputs = adapter.execute_data_minute_adjustment_snapshot_v1(case.context)
    actual = _read(case, outputs)[0]["snapshots"][code]
    baseline_financial = baseline.pop("financial_corporate_actions")
    assert [row["action_id"] for row in baseline_financial] == ["1:cash", "1:transfer"]
    assert actual.pop("financial_corporate_actions") == [baseline_financial[0]]
    assert actual == baseline
    assert "local/decisions.json#1" in outputs.completion_metadata.limitations[-1]

    exclusion["source_dates"]["transfer_arrival_date"] = "2024-01-05"
    with pytest.raises(ValueError, match="账户权益排除决策与源记录不符"):
        _snapshot(case)


def _bars(case, *, codes=CODES):
    staging = case.store.prepare()
    path = staging / 'bars' / 'raw.parquet'
    path.parent.mkdir()
    rows = [{'code': code, 'dt': datetime(2024, 1, 4, 9, 31), 'bar_start': datetime(2024, 1, 4, 9, 30, tzinfo=TZ), 'bar_end': datetime(2024, 1, 4, 9, 31, tzinfo=TZ), 'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.0, 'volume': 100.0, 'money': 1000.0, 'avg': 10.0} for code in sorted(codes)]
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)
    partition = inspect_parquet_partition(path, allowed_root=staging, partition_key='part-000', logical_start=START, logical_end=END, root_role='runtime_artifact', source_kind='runtime_derived', sort_keys=('code', 'dt'), lineage={'fixture': 'raw'})
    dataset = PartitionedDatasetRef(dataset_id='minute/raw/test', timestamp_field='dt', instrument_field='code', instruments=tuple(sorted(codes)), allowed_columns=tuple(table.column_names), partitions=(partition,), lineage={'fixture': 'raw'})
    payload = {'price_mode': 'raw', 'partitioned_dataset': dataset.to_dict(), 'source_snapshot_hash': dataset.reference_id, 'interval_minutes': 1}
    (staging / 'result.json').write_text(canonical_json(payload), encoding='utf-8')
    commit = case.store.commit(staging, artifact_name='bars', artifact_type='data.minute-bars.v1')
    return (RuntimeNodeValue.external(commit), payload)

def _adjust(case, snapshot_value, bars):
    context = _context(case.store, case.plans, {'mode': 'post', 'relative_tolerance': 1e-12}, inputs={'snapshot': snapshot_value, 'bars': bars}, port='bars', artifact_type='data.minute-bars.v1')
    return _read(case, adapter.execute_research_bars_minute_adjust_v1(context), 'bars')

def _publish_payload(case, payload):
    return _external_result(case.context, payload).values['snapshot']

def test_batch_snapshot_preserves_each_factor_actions_and_formal_parquet(tmp_path):
    case = _case(tmp_path)
    (payload, root, value) = _snapshot(case)
    assert payload['contract_version'] == 'runtime-minute-adjustment-snapshot-v2'
    members = payload['snapshots']
    assert list(members) == list(CODES)
    rows = pq.read_table(root / 'snapshot' / 'part-00000.parquet').to_pylist()
    assert [row['instrument_id'] for row in rows] == list(CODES)
    assert value.external_commit.row_counts['snapshot/part-00000.parquet'] == 2
    for (code, initial, close, cash) in zip(CODES, ('2', '3'), ('10', '20'), ('0.1', '0.4')):
        member = members[code]
        assert member['contract_version'] == 'runtime-minute-adjustment-snapshot-v1'
        snapshot = member['snapshot']
        assert snapshot['instrument_id'] == code
        assert Decimal(snapshot['initial_factor']) == Decimal(initial)
        assert Decimal(member['previous_closes']['2024-01-04']) == Decimal(close)
        assert Decimal(snapshot['factor_segments'][-1]['factor']) == Decimal(initial) * Decimal(close) / (Decimal(close) - Decimal(cash))
        assert len(member['included_actions']) == len(member['financial_corporate_actions']) == 1
        assert member['input_references'] == case.references
        row = next((item for item in rows if item['instrument_id'] == code))
        assert row['contract_version'] == 'runtime-minute-adjustment-snapshot-v2'
        assert json.loads(row['snapshot_json']) == snapshot
        assert json.loads(row['financial_corporate_actions_json']) == member['financial_corporate_actions']

def test_adjust_stream_uses_each_snapshot_and_collection_identity(tmp_path):
    case = _case(tmp_path)
    (snapshots, _, value) = _snapshot(case)
    (bars, raw) = _bars(case)
    (payload, root, _) = _adjust(case, value, bars)
    assert payload['price_mode'] == 'post'
    assert 'adjustment_snapshot' not in payload
    assert set(payload['adjustment_snapshots']) == set(CODES)
    dataset = payload['partitioned_dataset']
    rows = pq.read_table(root / dataset['partitions'][0]['relative_path']).to_pylist()
    for row in rows:
        code = row['code']
        member = snapshots['snapshots'][code]
        factor = Decimal(member['snapshot']['factor_segments'][-1]['factor'])
        assert row['close'] == pytest.approx(10 * float(factor))
        assert row['volume'] == pytest.approx(100 / float(factor))
        assert row['money'] == 1000
        assert row['factor_snapshot_hash'] == payload['adjustment_snapshots'][code]['adjustment_snapshot_identity_hash']
        assert payload['adjustment_snapshots'][code]['adjustment_candidates'] == member['candidate_actions']
    assert len(payload['financial_corporate_actions']) == 2
    assert payload['source_bars_snapshot_hash'] == raw['source_snapshot_hash']
    assert payload['source_snapshot_hash'] == typed_canonical_hash({'bars': raw['source_snapshot_hash'], 'adjustment_snapshots': payload['adjustment_snapshots']})
    changed = deepcopy(snapshots)
    changed['snapshots'][CODES[1]]['financial_corporate_actions'][0]['source_ref'] += '#changed'
    (updated, _, _) = _adjust(case, _publish_payload(case, changed), bars)
    assert updated['source_snapshot_hash'] != payload['source_snapshot_hash']

@pytest.mark.parametrize('change', ('missing', 'swap', 'broadcast', 'foreign_action', 'missing_financial'))
def test_adjust_rejects_incomplete_or_cross_instrument_members(tmp_path, change):
    case = _case(tmp_path)
    (payload, _, _) = _snapshot(case)
    members = payload['snapshots']
    if change == 'missing':
        del members[CODES[1]]
    elif change == 'swap':
        (members[CODES[0]], members[CODES[1]]) = (members[CODES[1]], members[CODES[0]])
    elif change == 'broadcast':
        members[CODES[1]] = deepcopy(members[CODES[0]])
    elif change == 'foreign_action':
        members[CODES[1]]['financial_corporate_actions'] = members[CODES[0]]['financial_corporate_actions']
    else:
        del members[CODES[1]]['financial_corporate_actions']
    (bars, _) = _bars(case)
    with pytest.raises(ValueError, match='身份|证券|权益'):
        _adjust(case, _publish_payload(case, payload), bars)

@pytest.mark.parametrize('updates', ({'instrument_id': CODES[0]}, {'instrument_ids': []}, {'instrument_ids': [CODES[0], CODES[0]]}, {'instrument_ids': '000001.XSHE'}, {'instrument_ids': [CODES[0], '']}, {'instrument_ids': [CODES[0], None]}))
def test_rejects_ambiguous_empty_or_duplicate_instrument_parameters(tmp_path, updates):
    case = _case(tmp_path)
    case.parameters.update(updates)
    with pytest.raises(ValueError, match='二选一|非空去重'):
        _snapshot(case)

def test_missing_prior_factor_fails_for_that_member(tmp_path):
    daily = [row for row in _daily() if not (row['fld_equity_daily_code'] == CODES[1] and row['fld_equity_daily_date'] < START.date())]
    case = _case(tmp_path, daily=daily)
    with pytest.raises(ValueError, match='前一已完成日 factor'):
        _snapshot(case)

@pytest.mark.parametrize("revised", [datetime(2024, 1, 4, 10), datetime(2025, 7, 12, 20)])
def test_stock_maintenance_time_does_not_delay_published_event(tmp_path, revised):
    case = _case(tmp_path, actions=[_action(CODES[1], 2, revised=revised)])
    payload, _, _ = _snapshot(case)
    action = payload['snapshots'][CODES[1]]['candidate_actions'][0]
    assert action['announcement_available_time'] == '2024-01-03T09:15:00+08:00'


def test_current_stock_source_rejects_conflicting_rows_for_same_event(tmp_path):
    original = _action(CODES[0], 1)
    later = _action(CODES[0], 1, cash='2.0', revised=datetime(2024, 1, 4, 10))
    with pytest.raises(QualityGateError, match='primary_key 重复'):
        _case(tmp_path, actions=[original, later])


@pytest.mark.parametrize("count", [1, 130])
def test_daily_and_action_sources_each_scan_once(tmp_path, monkeypatch, count):
    codes = tuple(f"{index:06d}.XSHE" for index in range(1, count + 1))
    actions = [_action(codes[0], 1)]
    if count > 1:
        actions.append({
            **_action(codes[-1], 2),
            "fld_ca_implementation_pub_date": date(2024, 1, 3),
        })
    case = _case(tmp_path, codes=codes, actions=actions)
    original = adapter._columnar_request_dataset
    daily_scans = []
    resolved_ids = []
    action_scans = []

    def resolve(context, request_id):
        resolved_ids.append(request_id)
        (dataset, reference, plan) = original(context, request_id)
        if request_id == DAILY_ID:
            scan = dataset.iter_batches

            def batches(**kwargs):
                daily_scans.append(kwargs)
                yield from scan(**kwargs)
            dataset.iter_batches = batches
        if request_id == ACTION_ID:
            scan = dataset._iter_ordered_file_batches

            def facts(**kwargs):
                action_scans.append(kwargs)
                yield from scan(**kwargs)
            dataset._iter_ordered_file_batches = facts
        return dataset, reference, plan

    monkeypatch.setattr(adapter, '_columnar_request_dataset', resolve)
    (payload, _, _) = _snapshot(case)
    assert len(payload['snapshots']) == count
    assert {code for code, member in payload["snapshots"].items() if member["candidate_actions"]} == {
        codes[0], codes[-1],
    }
    assert resolved_ids == [DAILY_ID, ACTION_ID]
    assert len(daily_scans) == 1
    assert daily_scans[0]['batch_size'] == 8192
    assert len(action_scans) == 1
    assert action_scans[0]["batch_size"] == 8192


def test_batch_snapshot_holds_one_read_only_connection_for_ordinary_database(tmp_path, monkeypatch):
    database = tmp_path / "market.duckdb"
    factor_database = tmp_path / "factor.duckdb"
    opened = []
    closed = []

    def connect(path, *, read_only):
        assert read_only is True
        opened.append(Path(path))
        return SimpleNamespace(close=lambda: closed.append(Path(path)))

    monkeypatch.setattr("duckdb.connect", connect)
    monkeypatch.setattr(adapter, "DirectVerifiedDataset", SimpleNamespace)
    monkeypatch.setattr(adapter, "_environment", lambda _: SimpleNamespace(source_databases={"factor": factor_database}))

    def source(path, *, source_kind="duckdb", factor_publication=None):
        return SimpleNamespace(
            manifest={"source_kind": source_kind, "database": str(path)},
            plan=SimpleNamespace(factor_publication=factor_publication),
        )

    with ExitStack() as connections:
        paths = set()
        for item in (
            source(database),
            source(database),
            source(factor_database),
            source(tmp_path / "publication.duckdb", factor_publication=object()),
            source(tmp_path / "archive", source_kind="archived_parquet"),
        ):
            adapter._hold_snapshot_database(None, item, connections, paths)
        assert opened == [database]
        assert not closed
    assert closed == [database]


def test_single_stock_payload_parquet_and_adjustment_fields_stay_v1(tmp_path):
    code = CODES[0]
    case = _case(tmp_path, codes=(code,), actions=[_action(code, 1)])
    del case.parameters['instrument_ids']
    case.parameters['instrument_id'] = code
    (payload, root, value) = _snapshot(case)
    assert payload['contract_version'] == 'runtime-minute-adjustment-snapshot-v1'
    assert 'snapshots' not in payload
    table = pq.read_table(root / 'snapshot' / 'part-00000.parquet')
    assert table.num_rows == 1
    assert 'instrument_id' not in table.column_names
    (bars, raw) = _bars(case, codes=(code,))
    (adjusted, _, _) = _adjust(case, value, bars)
    assert 'adjustment_snapshots' not in adjusted
    assert adjusted['adjustment_snapshot'] == payload['snapshot']
    assert adjusted['adjustment_candidates'] == payload['candidate_actions']
    assert adjusted['source_snapshot_hash'] == typed_canonical_hash({'bars': raw['source_snapshot_hash'], 'adjustment_snapshot': adjusted['adjustment_snapshot_identity_hash'], 'adjustment_candidates': payload['candidate_actions'], 'adjustment_effective_times': payload['effective_times'], 'adjustment_previous_closes': payload['previous_closes']})

def test_batch_listing_evidence_keeps_ipo_origin_separate_and_parquet_column(tmp_path):
    daily = [row for row in _daily() if not (row['fld_equity_daily_code'] == CODES[1] and row['fld_equity_daily_date'] < START.date())]
    case = _case(tmp_path, daily=daily, actions=[_action(CODES[0], 1)])
    evidence = {'instrument_id': CODES[1], 'listed_date': '2024-01-02', 'available_at': '2024-01-02T08:00:00+08:00', 'source_reference': 'fixture:listing'}
    case.parameters['initial_listing_evidence_by_instrument'] = {CODES[1]: evidence}
    (payload, root, _) = _snapshot(case)
    (first, ipo) = (payload['snapshots'][code] for code in CODES)
    assert Decimal(first['snapshot']['initial_factor']) == 2
    assert Decimal(ipo['snapshot']['initial_factor']) == 1
    assert ipo['initial_listing_evidence'] == evidence
    assert 'initial_listing_evidence' not in first
    rows = pq.read_table(root / 'snapshot' / 'part-00000.parquet').to_pylist()
    assert rows[0]['initial_listing_evidence_json'] is None
    assert json.loads(rows[1]['initial_listing_evidence_json']) == evidence
    assert ipo['snapshot']['source_revision_hash'] == typed_canonical_hash({'input_references': case.references, 'initial_listing_evidence': evidence})

@pytest.mark.parametrize('listing', ([], {'other.XSHE': {}}, {CODES[1]: {'instrument_id': CODES[0], 'listed_date': '2024-01-02', 'available_at': '2024-01-02T08:00:00+08:00', 'source_reference': 'fixture:listing'}}))
def test_batch_listing_evidence_rejects_wrong_members_and_shape(tmp_path, listing):
    case = _case(tmp_path)
    case.parameters['initial_listing_evidence_by_instrument'] = listing
    with pytest.raises(ValueError, match='映射|不一致'):
        _snapshot(case)

def test_relevant_daily_duplicate_is_rejected_before_snapshot_build(tmp_path, monkeypatch):
    case = _case(tmp_path)
    original = adapter._columnar_request_dataset

    def resolve(context, request_id):
        (dataset, reference, plan) = original(context, request_id)
        if request_id == DAILY_ID:
            scan = dataset.iter_batches

            def duplicated(**kwargs):
                for batch in scan(**kwargs):
                    yield batch
                    yield batch
            dataset.iter_batches = duplicated
        return (dataset, reference, plan)
    monkeypatch.setattr(adapter, '_columnar_request_dataset', resolve)
    with pytest.raises(ValueError, match='日线重复'):
        _snapshot(case)

def test_absent_daily_member_is_rejected_even_with_other_member_complete(tmp_path):
    daily = [row for row in _daily() if row['fld_equity_daily_code'] != CODES[1]]
    case = _case(tmp_path, daily=daily, actions=[_action(CODES[0], 1)])
    with pytest.raises(ValueError, match='缺少当前标的日线'):
        _snapshot(case)

def test_single_member_batch_stays_v2_and_empty_actions_keep_constant_factor(tmp_path):
    code = CODES[0]
    case = _case(tmp_path, codes=(code,), actions=[])
    (payload, _, value) = _snapshot(case)
    assert payload['contract_version'] == 'runtime-minute-adjustment-snapshot-v2'
    member = payload['snapshots'][code]
    assert member['candidate_actions'] == member['included_actions'] == member['financial_corporate_actions'] == []
    assert member['previous_closes'] == member['effective_times'] == {}
    assert len(member['snapshot']['factor_segments']) == 1
    (bars, _) = _bars(case, codes=(code,))
    (adjusted, _, _) = _adjust(case, value, bars)
    assert set(adjusted['adjustment_snapshots']) == {code}
    assert 'adjustment_snapshot' not in adjusted

def test_batch_keeps_policy_visibility_and_member_isolation(tmp_path):
    case = _case(tmp_path)
    (payload, _, _) = _snapshot(case)
    assert all((len(member['candidate_actions']) == 1 for member in payload['snapshots'].values()))
    assert payload['snapshots'][CODES[0]]['candidate_actions'][0]['instrument_hash'] != payload['snapshots'][CODES[1]]['candidate_actions'][0]['instrument_hash']

def test_batch_calendar_does_not_delay_visibility_for_member_missing_daily_row(tmp_path):
    daily = [row for row in _daily() if not (row['fld_equity_daily_code'] == CODES[0] and row['fld_equity_daily_date'] == date(2024, 1, 3))]
    case = _case(tmp_path, daily=daily)
    (payload, _, _) = _snapshot(case)
    action = payload['snapshots'][CODES[0]]['candidate_actions'][0]
    assert action['announcement_available_time'] == '2024-01-03T09:15:00+08:00'
    assert payload['snapshots'][CODES[0]]['previous_closes']['2024-01-04'] == '10.0'

def test_changing_one_member_updates_collection_identity_without_borrowing_factors(tmp_path):
    case = _case(tmp_path / 'first')
    (_, _, first) = _snapshot(case)
    (bars, _) = _bars(case)
    (original, _, _) = _adjust(case, first, bars)
    daily = _daily()
    for row in daily:
        if row['fld_equity_daily_code'] == CODES[1]:
            row['fld_equity_daily_factor'] = 4.0
    other = _case(tmp_path / 'second', daily=daily)
    (second_payload, _, _) = _snapshot(other)
    second = _publish_payload(case, second_payload)
    (changed, root, _) = _adjust(case, second, bars)
    assert original['adjustment_snapshot_identity_hash'] != changed['adjustment_snapshot_identity_hash']
    assert original['source_snapshot_hash'] != changed['source_snapshot_hash']
    assert original['source_bars_snapshot_hash'] == changed['source_bars_snapshot_hash']
    dataset = changed['partitioned_dataset']
    assert dataset['lineage']['snapshot_identity_hash'] == changed['adjustment_snapshot_identity_hash']
    rows = pq.read_table(root / dataset['partitions'][0]['relative_path']).to_pylist()
    assert rows[0]['factor'] == pytest.approx(20 / 9.9)
    assert rows[1]['factor'] == pytest.approx(80 / 19.6)

@pytest.mark.parametrize("source_codes", (("510300", "159919"), ("510300.XSHG", "159919.XSHE")))
def test_temporal_batch_keeps_etf_normalization_and_revisions(source_codes):
    instruments = ("159919.XSHE", "510300.XSHG")
    rows = [
        {
            "fld_etf_ca_id": index + 1,
            "fld_etf_ca_code": code,
            "fld_etf_ca_pub_date": date(2024, 1, 2),
            "fld_etf_ca_add_time": datetime(2024, 1, 2, 8),
            "fld_etf_ca_mod_time": datetime(2024, 1, 2, 8),
            "fld_etf_ca_ex_date": date(2024, 1, 4),
            "fld_etf_ca_cash_per_share": Decimal("0.10123456"),
        }
        for index, code in enumerate(source_codes)
    ]
    correction = {
        **rows[0],
        "fld_etf_ca_mod_time": datetime(2024, 1, 4, 8),
        "fld_etf_ca_cash_per_share": Decimal("0.20123456"),
    }
    plan = _plan({
        "dataset_id": "cn_etf.corporate_action",
        "field_ids": tuple(rows[0]),
        "sort": [{"field_id": "fld_etf_ca_id"}],
        "adjustment": "unadjusted",
    }, instruments)
    table = pa.Table.from_pylist([rows[0], correction, rows[1]])
    scans = []

    class Dataset:
        schema = table.schema
        max_batch_rows = 8192
        max_batch_bytes = 1024**2
        manifest = {"temporal_source": True, "admitted_plan_hash": plan.plan_hash}

        def _iter_ordered_file_batches(self, **kwargs):
            scans.append(kwargs)
            yield from table.select(kwargs["columns"]).to_batches(max_chunksize=1)

    reference = object()
    result, actual_reference = adapter._temporal_corporate_action_rows_many(
        SimpleNamespace(effective_resource_budget=SimpleNamespace(memory_bytes=256 * 1024**2)),
        "etf",
        instrument_ids=instruments,
        asset_class="cn_etf",
        trading_sessions=tuple(date(2024, 1, day) for day in (2, 3, 4, 5)),
        applicable_start=START,
        applicable_end=END,
        as_of=END,
        initial_listing_dates={},
        action_source=(Dataset(), reference, plan),
    )
    assert len(scans) == 1
    assert actual_reference is reference
    assert result == {
        "159919.XSHE": ({**rows[1], "__revision": 1},),
        "510300.XSHG": ({**rows[0], "__revision": 1}, {**correction, "__revision": 2}),
    }


@pytest.mark.parametrize('with_ipo', (False, True))
def test_snapshot_handoff_matches_strategy_context_and_formal_parquet(tmp_path, with_ipo):
    from research_pipeline.runtime.adapters.minute_strategy_pit import strategy_snapshot_context
    daily = _daily()
    actions = [_action(CODES[0], 1), _action(CODES[1], 2, cash='4.0')]
    if with_ipo:
        daily = [row for row in daily if not (row['fld_equity_daily_code'] == CODES[1] and row['fld_equity_daily_date'] < START.date())]
        actions = actions[:1]
    case = _case(tmp_path, daily=daily, actions=actions)
    if with_ipo:
        case.parameters['initial_listing_evidence_by_instrument'] = {CODES[1]: {'instrument_id': CODES[1], 'listed_date': '2024-01-02', 'available_at': '2024-01-02T08:00:00+08:00', 'source_reference': 'fixture:listing'}}
    (payload, root, value) = _snapshot(case)
    (bars, raw) = _bars(case)
    (adjusted, _, _) = _adjust(case, value, bars)
    strategy_context = strategy_snapshot_context(payload, source_bars_hash=raw['source_snapshot_hash'])
    assert strategy_context['adjustment_snapshots'] == adjusted['adjustment_snapshots']
    assert strategy_context['financial_corporate_actions'] == adjusted['financial_corporate_actions']
    assert strategy_context['source_bars_snapshot_hash'] == adjusted['source_bars_snapshot_hash']
    table = pq.read_table(root / 'snapshot' / 'part-00000.parquet')
    rows = table.to_pylist()
    assert {row["instrument_id"] for row in rows} == set(payload["snapshots"])
    for row in rows:
        member = payload["snapshots"][row["instrument_id"]]
        assert json.loads(row["snapshot_json"]) == member["snapshot"]
        assert json.loads(row["financial_corporate_actions_json"]) == member["financial_corporate_actions"]

@pytest.mark.parametrize('revised', [datetime(2024, 1, 3, 8), datetime(2024, 1, 4, 8), datetime(2024, 1, 4, 9, 31)])
def test_first_stored_revision_visible_before_effect_is_sufficient(tmp_path, revised):
    case = _case(tmp_path, actions=[_action(CODES[0], 1, revised=revised)])
    (payload, root, _) = _snapshot(case)
    member = payload['snapshots'][CODES[0]]
    visible = datetime(2024, 1, 3, 9, 15, tzinfo=TZ)
    assert member['candidate_actions'][0]['announcement_available_time'] == visible.isoformat()
    segments = member['snapshot']['factor_segments']
    assert segments[0]['factor'] == '2.0'
    assert segments[1]['starts_at'] == '2024-01-04T09:31:00+08:00'
    assert Decimal(segments[1]['factor']) > Decimal('2')

def test_stock_added_time_does_not_delay_published_event(tmp_path):
    row = _action(CODES[0], 1, revised=datetime(2025, 1, 4, 11))
    row['fld_ca_add_time'] = datetime(2025, 1, 4, 10)
    case = _case(tmp_path, actions=[row])
    payload, _, _ = _snapshot(case)
    assert payload['snapshots'][CODES[0]]['candidate_actions'][0]['announcement_available_time'] == '2024-01-03T09:15:00+08:00'


@pytest.mark.parametrize('old_day', [date(2023, 12, 29), date(2024, 1, 3), date(2024, 1, 5)])
def test_known_changed_effective_date_cannot_be_hidden_by_window_filter(tmp_path, old_day):
    original = _action(CODES[0], 1)
    original['fld_ca_ex_date'] = old_day
    revision = _action(CODES[0], 1, revised=datetime(2024, 1, 3, 8))
    with pytest.raises(QualityGateError, match='primary_key 重复'):
        _case(tmp_path, actions=[original, revision])


def test_transfer_not_distributed_to_original_shareholders_has_no_entitlement(tmp_path):
    row = _action(CODES[0], 244485, cash='0')
    row['fld_ca_transfer_per_ten'] = Decimal('10')
    row['fld_ca_note'] = '2021-09-13：本次转增不向原股东分配。'
    case = _case(tmp_path, actions=[row])
    payload, _, _ = _snapshot(case)
    member = payload['snapshots'][CODES[0]]
    assert member['candidate_actions'] == []
    assert member['financial_corporate_actions'] == []


def test_stock_publication_day_is_not_visible_to_consumer(tmp_path):
    case = _case(tmp_path)
    dataset, _, plan = adapter._columnar_request_dataset(case.context, ACTION_ID)
    rows = [row for batch in dataset.iter_batches_at(
        plan=plan, consumer_time='2024-01-02T15:00:00+08:00',
        columns=plan.query.field_ids, batch_size=8192,
    ) for row in batch.to_pylist()]
    assert rows == []
