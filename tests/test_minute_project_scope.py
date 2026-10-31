"""项目分钟范围准入与历史可见性；只使用内存对象和文件。"""
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
import pytest
from research_pipeline.domain import SessionPolicyBundle, SessionCalendarResolver, MinuteRuleSnapshotBundle, MinuteRuleResolver, load_session_policy_bundle
from research_pipeline.domain.minute_request_scope import require_minute_request_coverage
from research_pipeline.domain.minute_rule_snapshots import PROJECT_RULE_BUNDLE_VERSION, MinuteRuleSnapshotError
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION, PROJECT_SESSION_POLICY_VERSION, SessionCalendarError
from research_pipeline.platform import typed_canonical_hash
from minute_rule_fixture import _bundle
CODE = '000002.XSHE'
OTHER = '000003.XSHE'
START = datetime.fromisoformat('2024-01-08T09:30:00+08:00')
END = datetime.fromisoformat('2024-01-09T15:01:00+08:00')

def seal(payload, cls):
    payload['bundle_hash'] = typed_canonical_hash({key: value for (key, value) in payload.items() if key != 'bundle_hash'})
    return cls.from_dict(payload)

def project_inputs(codes=(CODE, OTHER)):
    calendar = load_session_policy_bundle().to_dict()
    prototype = next((item for item in calendar['policies'] if item['instrument']['instrument_id'] == '000001.XSHE' and item['revision'] == 1))
    policies = []
    for code in codes:
        item = deepcopy(prototype)
        item.update(contract_version=PROJECT_SESSION_POLICY_VERSION, available_at='2024-01-01T00:00:00+08:00', effective_from='2024-01-08', effective_to='2024-01-09', trading_dates=['2024-01-08', '2024-01-09'])
        item['instrument']['instrument_id'] = code
        item['instrument']['metadata_hash'] = typed_canonical_hash({'source': 'fixture-stock-classification', 'code': code})
        item['evidence_hashes'].append({'source_id': 'fixture.project.calendar', 'content_hash': 'e' * 64})
        item['evidence_hashes'].sort(key=lambda row: row['source_id'])
        policies.append(item)
    calendar.update(contract_version=PROJECT_SESSION_BUNDLE_VERSION, policies=policies)
    sessions = seal(calendar, SessionPolicyBundle)
    rules = _bundle().to_dict()
    prototypes = [item for item in rules['rules'] if item['instrument_id'] == '000001.XSHE']
    rows = []
    for code in codes:
        for prototype in prototypes:
            item = deepcopy(prototype)
            item.update(instrument_id=code, effective_from='2024-01-08', effective_to='2024-01-09')
            if item['rule_id'].endswith('.session.v1'):
                item['parameters']['session_policy_id'] = policies[0]['policy_id']
            if item['rule_id'].endswith('.price_limit.v1'):
                for (revision, day, previous) in ((1, '2024-01-08', '2024-01-05'), (2, '2024-01-09', '2024-01-08')):
                    daily = deepcopy(item)
                    daily.update(revision=revision, effective_from=day, effective_to=day, available_at=previous + 'T15:00:00+08:00')
                    daily['parameters']['reference_price_available_at'] = previous + 'T15:00:00+08:00'
                    rows.append(daily)
            else:
                rows.append(item)
    rules.update(contract_version=PROJECT_RULE_BUNDLE_VERSION, instruments=[item['instrument'] for item in policies], rules=rows)
    for source in rules['sources']:
        if source['source_kind'] == 'repository_snapshot':
            source['locator'] = 'fixture://project-calendar#' + sessions.bundle_hash
    return (sessions, seal(rules, MinuteRuleSnapshotBundle))

def admit(sessions, rules, **changes):
    return require_minute_request_coverage(rules, sessions, **{**dict(instruments=(CODE,), start_at=START, end_at=END), **changes})

def test_new_instrument_dates_and_subset_are_covered_without_editing_reference_manifest():
    (sessions, rules) = project_inputs()
    admit(sessions, rules)
    assert {item.instrument_id for item in rules.instruments} == {CODE, OTHER}
    assert len(sessions.policies) == 2
    assert SessionPolicyBundle.from_dict(sessions.to_dict()) == sessions
    assert MinuteRuleSnapshotBundle.from_dict(rules.to_dict()) == rules

@pytest.mark.parametrize('change,pattern', [({'instruments': ('000004.XSHE',)}, '证券缺失'), ({'end_at': datetime.fromisoformat('2024-01-10T15:00:00+08:00')}, '日期覆盖缺失')])
def test_missing_requested_scope_is_rejected(change, pattern):
    (sessions, rules) = project_inputs()
    with pytest.raises((MinuteRuleSnapshotError, SessionCalendarError), match=pattern):
        admit(sessions, rules, **change)

def test_missing_rule_reports_instrument_and_rule():
    (_, rules) = project_inputs()
    payload = rules.to_dict()
    payload['rules'] = [row for row in payload['rules'] if not (row['instrument_id'] == CODE and row['rule_id'].endswith('.settlement.v1'))]
    with pytest.raises(MinuteRuleSnapshotError, match=CODE + '.*settlement'):
        seal(payload, MinuteRuleSnapshotBundle)

def test_future_only_rule_cannot_cover_past_request():
    (sessions, rules) = project_inputs()
    payload = rules.to_dict()
    payload['rules'][0]['available_at'] = '2024-01-10T00:00:00+08:00'
    with pytest.raises(MinuteRuleSnapshotError, match='请求规则覆盖失败'):
        admit(sessions, seal(payload, MinuteRuleSnapshotBundle))

def test_rule_revision_stream_is_retained_and_selected_as_of():
    (sessions, rules) = project_inputs()
    payload = rules.to_dict()
    original = next((row for row in payload['rules'] if row['instrument_id'] == CODE and row['rule_id'].endswith('.trading_fee.v1')))
    later = deepcopy(original)
    later.update(revision=2, available_at='2024-01-09T00:00:00+08:00')
    later['parameters']['commission_ppm'] = 300
    payload['rules'].append(later)
    payload['rules'].sort(key=lambda row: (row['instrument_id'], row['rule_id'], row['revision']))
    updated = seal(payload, MinuteRuleSnapshotBundle)
    admit(sessions, updated)
    resolver = MinuteRuleResolver(updated)
    assert resolver.resolve(rule_id=original['rule_id'], instrument_id=CODE, effective_on=START.date(), as_of=START).rule.revision == 1
    assert resolver.resolve(rule_id=original['rule_id'], instrument_id=CODE, effective_on=END.date(), as_of=END).rule.revision == 2
    assert len(updated.rules) == len(rules.rules) + 1

def test_future_calendar_is_not_used_for_past_sessions():
    (sessions, rules) = project_inputs()
    payload = sessions.to_dict()
    payload['policies'][0]['available_at'] = '2024-01-10T00:00:00+08:00'
    sessions = seal(payload, SessionPolicyBundle)
    raw = rules.to_dict()
    for source in raw['sources']:
        if source['source_kind'] == 'repository_snapshot':
            source['locator'] = 'fixture://project-calendar#' + sessions.bundle_hash
    with pytest.raises(SessionCalendarError, match='可见版本缺失'):
        admit(sessions, seal(raw, MinuteRuleSnapshotBundle))

def test_calendar_revision_is_selected_at_bar_time_and_full_chain_remains():
    (sessions, _) = project_inputs(codes=(CODE,))
    payload = sessions.to_dict()
    older = sessions.policies[0]
    later = deepcopy(payload['policies'][0])
    later.update(revision=2, previous_policy_hash=older.policy_hash, available_at='2024-01-09T00:00:00+08:00')
    payload['policies'].append(later)
    updated = seal(payload, SessionPolicyBundle)
    resolver = SessionCalendarResolver(updated)
    for (at, revision) in (('2024-01-08T09:31:00+08:00', 1), ('2024-01-09T09:31:00+08:00', 2)):
        result = resolver.resolve_completed_bar_end(older.instrument, datetime.fromisoformat(at), policy_revision=None)
        assert '@' + str(revision) + ':' in result.session_id
    assert len(updated.policies) == 2

def test_calendar_source_and_previous_close_are_not_interchangeable():
    (sessions, rules) = project_inputs()
    payload = rules.to_dict()
    for source in payload['sources']:
        if source['source_kind'] == 'repository_snapshot':
            source['locator'] = 'fixture://different#' + 'f' * 64
    with pytest.raises(MinuteRuleSnapshotError, match='日历来源'):
        admit(sessions, seal(payload, MinuteRuleSnapshotBundle))
    payload = rules.to_dict()
    for row in payload['rules']:
        if row['instrument_id'] == CODE and row['rule_id'].endswith('.price_limit.v1'):
            row['parameters']['reference_price_available_at'] = END.isoformat()
    with pytest.raises(MinuteRuleSnapshotError, match='前收盘价尚不可见'):
        admit(sessions, seal(payload, MinuteRuleSnapshotBundle))

def _project_package(tmp_path):
    import shutil
    import yaml
    from pathlib import Path
    (sessions, rules) = project_inputs()
    source = Path(__file__).parents[1] / 'research_packages/references/minute_stock_session'
    target = tmp_path / 'project'
    shutil.copytree(source, target)
    spec = target / 'spec/research.yaml'
    payload = yaml.safe_load(spec.read_text(encoding='utf-8'))
    payload['as_of'] = END.isoformat()
    for request in payload['requests']:
        request['universe']['instruments'] = [CODE]
        if request['dataset_id'].endswith('minute_bar'):
            request['time_range'].update(start_at=START.isoformat(), end_at=END.isoformat())
        else:
            request['time_range'].update(start='2024-01-05', end='2024-01-09')
    from research_pipeline.domain.minute_scope_input import write_minute_scope_input
    reference = write_minute_scope_input(target, name='market', calendar=sessions, rules=rules)
    for node in payload['graph']['nodes']:
        if node['operator_id'] == 'research.bars.minute_resample':
            node['parameters']['minute_scope'] = reference
        if node['operator_id'] == 'finance.simulation.intraday':
            node['parameters'].update(minute_scope=reference, rule_bundle_hash=rules.bundle_hash)
    spec.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding='utf-8')
    return (target, spec, payload, sessions, rules)

def test_public_package_compilation_freezes_project_scope(tmp_path):
    import yaml
    from research_pipeline.packages import compile_research_package, load_research_package, ResearchPackageError
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
    (target, spec, payload, sessions, rules) = _project_package(tmp_path)
    registry = build_mainline_operator_registry()
    plan = compile_research_package(load_research_package(target), admission=registry)
    assert load_research_package(target).minute_scope_inputs['market'].rules.bundle_hash == rules.bundle_hash
    bars = next((node for node in plan.recipe.nodes if node.operator_id == 'research.bars.minute_resample'))
    assert bars.parameters['minute_scope']['calendar_hash'] == sessions.bundle_hash
    assert load_research_package(target).minute_scope_inputs['market'].calendar == sessions
    for request in payload['requests']:
        if request['dataset_id'].endswith('minute_bar'):
            request['universe']['instruments'] = ['000004.XSHE']
    spec.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding='utf-8')
    with pytest.raises(ResearchPackageError, match='证券缺失'):
        compile_research_package(load_research_package(target), admission=registry)

def test_project_scope_is_checked_on_bars_branch_only(tmp_path):
    from research_pipeline.packages.minute_scope_admission import validate_minute_scope_graph
    (sessions, rules) = project_inputs()
    from research_pipeline.domain.minute_scope_input import write_minute_scope_input, load_minute_scope_input
    reference = write_minute_scope_input(tmp_path, name='market', calendar=sessions, rules=rules)
    scope_inputs = {'market': load_minute_scope_input(tmp_path, reference)}

    def node(name, operator, parameters, inputs=()):
        return SimpleNamespace(node_id=name, operator_id=operator, parameters=parameters, inputs=tuple((SimpleNamespace(input_port=port, source_node_id=source) for (port, source) in inputs)))
    scan = node('scan', 'data.minute.scan', {'request_ids': ['prices']})
    unrelated = node('unrelated', 'data.minute.scan', {'request_ids': ['unrelated']})
    bars = node('bars', 'research.bars.minute_resample', {'session_policy_ref': sessions.policies[0].policy_id, 'minute_scope': reference}, (('minute_1m', 'scan'),))
    adjusted = node('adjusted', 'research.bars.minute_adjust', {}, (('bars', 'bars'), ('snapshot', 'unrelated')))
    simulation = node('sim', 'finance.simulation.intraday', {'minute_scope': reference, 'rule_bundle_hash': rules.bundle_hash}, (('bars', 'adjusted'),))
    recipe = SimpleNamespace(nodes=(scan, unrelated, bars, adjusted, simulation))
    query = SimpleNamespace(universe=SimpleNamespace(instruments=(CODE,)), time_range=SimpleNamespace(start_at=START, end_at=END))
    validate_minute_scope_graph(recipe, {'prices': query}, scope_inputs=scope_inputs)
    bars.parameters.pop('minute_scope')
    from research_pipeline.packages import ResearchPackageError
    with pytest.raises(ResearchPackageError, match='唯一行情日历'):
        validate_minute_scope_graph(recipe, {'prices': query}, scope_inputs=scope_inputs)

def test_project_calendar_is_consumed_by_resampling_and_artifact_loader(tmp_path, monkeypatch):
    from research_pipeline.data_plane import MinuteScanPlan, MinuteScanBudget, MinuteSourcePartition, build_minute_resample_plan
    from research_pipeline.runtime.adapters.minute_io import _session_bundle_from_payload
    from tests.test_minute_resampling import _Batches, _collect
    from datetime import timedelta
    (sessions, _) = project_inputs(codes=(CODE,))
    instrument = sessions.policies[0].instrument
    scan = MinuteScanPlan('a' * 64, 'stock', ('code', 'dt', 'open', 'high', 'low', 'close', 'volume', 'money', 'avg'), (CODE,), START, END, END, (MinuteSourcePartition('stock/year=2024/month=01/data.parquet', 1, 1, 1, 1, 'b' * 64),), 1000, 1000000, 100, MinuteScanBudget(), 'cn_stock', sessions.policies[0].policy_id, ('quality.minute.common_fields.v1', 'quality.minute.stock.v1'), dict(sessions.capability_binding)['minute_capability_manifest_hash'])
    plan = build_minute_resample_plan(scan, interval_minutes=1, session_bundle=sessions, instruments=(instrument,))
    rows = [{'code': CODE, 'dt': (START + timedelta(minutes=i)).replace(tzinfo=None), 'open': 10.0, 'high': 10.0, 'low': 10.0, 'close': 10.0, 'volume': 100.0, 'money': 1000.0, 'avg': 10.0} for i in range(1, 6)]
    (output, _) = _collect(_Batches(rows, 2), plan)
    assert len(output) == 5
    assert output[0]['code'] == CODE
    assert sum(row['volume'] for row in output) == 500
    assert plan.policy_revision is None
    from research_pipeline.runtime.adapters import minute_io
    reference = {'name': 'market', 'calendar_hash': sessions.bundle_hash}
    context = SimpleNamespace(environment=SimpleNamespace(minute_scope_inputs={'market': SimpleNamespace(reference=reference, calendar=sessions)}))
    monkeypatch.setattr(minute_io, '_environment', lambda ctx: ctx.environment)
    assert _session_bundle_from_payload({'minute_scope': reference, 'session_policy_bundle_hash': sessions.bundle_hash}, context) == sessions
    with pytest.raises(ValueError, match='冻结身份'):
        _session_bundle_from_payload({'minute_scope': reference, 'session_policy_bundle_hash': 'a' * 64}, context)



@pytest.mark.parametrize('interval', [1, 5])
def test_runtime_resample_preserves_project_calendar_and_consumes_it(tmp_path, monkeypatch, interval):
    import json
    from datetime import timedelta
    import pyarrow as pa
    import pyarrow.parquet as pq
    from research_pipeline.packages import compile_research_package, load_research_package
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
    from research_pipeline.runtime import ExternalArtifactStore
    from research_pipeline.runtime.adapters import minute_data, minute_io
    from research_pipeline.runtime.adapters.common import ResearchRunEnvironment
    from research_pipeline.data_plane import build_minute_scan_plan, build_minute_partitioned_dataset
    from tests.test_minute_runtime_adapters import _admit
    from tests.test_minute_resampling import _source_schema
    (target, _, _, sessions, _) = _project_package(tmp_path)
    package = load_research_package(target)
    plan = compile_research_package(package, admission=build_mainline_operator_registry())
    admitted = _admit(plan, request_id='stock_minute')
    root = tmp_path / 'minute-data'
    source = root / 'stock/year=2024/month=01'
    source.mkdir(parents=True)
    rows = [{'code': CODE, 'dt': (START + timedelta(minutes=i)).replace(tzinfo=None), 'open': 10.0, 'high': 10.0, 'low': 10.0, 'close': 10.0, 'volume': 100.0, 'money': 1000.0, 'avg': 10.0} for i in range(1, 6)]
    pq.write_table(pa.Table.from_pylist(rows, schema=_source_schema()), source / 'data.parquet')
    scan = build_minute_scan_plan(admitted, source=root / 'stock', allowed_root=root)
    dataset = build_minute_partitioned_dataset(scan)
    payload = {'quality_status': 'pending_stream_consumption', 'request_id': 'stock_minute', 'partitioned_dataset': dataset.to_dict(), 'source_snapshot_hash': dataset.reference_id}
    parameters = {node.node_id: dict(node.parameters) for node in plan.recipe.nodes}
    bars_node = next((node for node in plan.recipe.nodes if node.operator_id == 'research.bars.minute_resample'))
    parameters[bars_node.node_id]['interval_minutes'] = interval
    environment = ResearchRunEnvironment(tmp_path, {}, {'stock_minute': admitted}, None, {}, root, tmp_path / 'artifacts', parameters, None, plan.fixed_clock, plan.root_seed, 1, None, {}, package.minute_scope_inputs)
    context = SimpleNamespace(environment=environment, external_store=ExternalArtifactStore(tmp_path / 'external'), node=SimpleNamespace(node_id=bars_node.node_id, output_types=(('bars', 'data.minute-bars.v1'),)), effective_resource_budget=SimpleNamespace(memory_bytes=1024 ** 3))
    monkeypatch.setattr(minute_data, '_input_external_payload', lambda context, port: payload)
    if interval != 1:
        with pytest.raises(ValueError, match='仅支持一分钟'):
            minute_data.execute_research_bars_minute_resample_v1(context)
        return
    outputs = minute_data.execute_research_bars_minute_resample_v1(context)
    commit = outputs.values['bars'].external_commit
    artifact = context.external_store.objects_root / commit.semantic_hash
    saved = json.loads((artifact / 'result.json').read_text(encoding='utf-8'))
    assert saved['minute_scope']['calendar_hash'] == sessions.bundle_hash
    assert 'session_policy_bundle' not in saved
    monkeypatch.setattr(minute_io, '_input_external_root', lambda context, port: artifact)
    (_, partitions) = minute_io._operator_bar_partitions(context, saved)
    actual = [row for (_, stream) in partitions for row in stream]
    assert len(actual) == 5 // interval
    assert all((row['instrument'] == CODE and row['session_id'].endswith(f':{CODE}:2024-01-08') for row in actual))
    assert sum((row['volume'] for row in actual)) == 500

def test_daily_reference_price_cannot_be_reused_as_period_rule():
    (sessions, rules) = project_inputs()
    payload = rules.to_dict()
    for row in payload['rules']:
        if row['instrument_id'] == CODE and row['rule_id'].endswith('.price_limit.v1') and (row['revision'] == 1):
            row['effective_to'] = '2024-01-09'
    with pytest.raises(MinuteRuleSnapshotError, match='逐交易日覆盖'):
        admit(sessions, seal(payload, MinuteRuleSnapshotBundle))





@pytest.mark.parametrize('revision', [False, True])
def test_request_session_boundaries_keep_visible_daily_policy(revision):
    from research_pipeline.domain.minute_request_scope import minute_request_sessions
    (sessions, _) = project_inputs(codes=(CODE,))
    if revision:
        payload = sessions.to_dict()
        later = deepcopy(payload['policies'][0])
        later.update(revision=2, previous_policy_hash=sessions.policies[0].policy_hash, available_at='2024-01-09T00:00:00+08:00')
        later['segments'][-1]['end_time'] = '14:30:00'
        payload['policies'].append(later)
        sessions = seal(payload, SessionPolicyBundle)
    rows = list(minute_request_sessions(sessions, instruments=(CODE,), start_at=START, end_at=END))
    assert len(rows) == 2
    for (policy, day, starts, ends) in rows:
        full = policy.build_session(day, scope_binding_hash=sessions.capability_binding_hash)
        eligible = [segment for segment in full.segments if segment.bar_eligible]
        assert starts == max(START, min((segment.starts_at for segment in eligible)))
        assert ends == min(END, max((segment.ends_at for segment in eligible)))
    assert [policy.revision for (policy, *_) in rows] == ([1, 2] if revision else [1, 1])
