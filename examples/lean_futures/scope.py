"""定义公开合成期货的会话、分仓规则与独立结算事实。"""
from copy import deepcopy
from research_pipeline.domain import SessionPolicyBundle, load_session_policy_bundle
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION, PROJECT_SESSION_POLICY_VERSION
from research_pipeline.domain.minute_rule_snapshots import MinuteRuleSnapshotBundle, PROJECT_RULE_BUNDLE_VERSION, load_minute_rule_snapshot_bundle
from research_pipeline.domain.minute_scope_input import write_minute_scope_input
from research_pipeline.platform import typed_canonical_hash
import synthetic


def seal(value, cls):
    value['bundle_hash'] = typed_canonical_hash({key: item for key, item in value.items() if key != 'bundle_hash'})
    return cls.from_dict(value)


def write_scope(package):
    calendar = load_session_policy_bundle().to_dict()
    template = next(row for row in calendar['policies'] if row['instrument']['instrument_id'] == 'IF2401.CCFX')
    source = typed_canonical_hash({'days': synthetic.DAYS, 'codes': synthetic.CODES, 'multiplier': 10, 'fee_units': 100})
    policies = []
    visible = synthetic.DAYS[0]+'T00:00:00+08:00'
    for code in synthetic.CODES:
        row = deepcopy(template)
        row.update(policy_id='session.public.synthetic.future.v1', revision=1,
                   contract_version=PROJECT_SESSION_POLICY_VERSION, effective_from=synthetic.DAYS[0],
                   effective_to=synthetic.DAYS[-1], trading_dates=list(synthetic.DAYS), available_at=visible,
                   previous_policy_hash=None)
        row['instrument'].update(instrument_id=code, metadata_hash=typed_canonical_hash({'code': code, 'source': source}))
        row['evidence_hashes'] = [value for value in row['evidence_hashes'] if value['source_id'].startswith('minute.capability.')]
        row['evidence_hashes'].append(dict(source_id='public.synthetic.futures', content_hash=source))
        row['evidence_hashes'].sort(key=lambda value: value['source_id'])
        policies.append(row)
    calendar.update(bundle_id='public.synthetic.futures.calendar', contract_version=PROJECT_SESSION_BUNDLE_VERSION, policies=policies)
    calendar = seal(calendar, SessionPolicyBundle)
    original = load_minute_rule_snapshot_bundle().to_dict()
    rules = dict(capability_binding=original['capability_binding'], bundle_id='public.synthetic.futures.rules',
        contract_version=PROJECT_RULE_BUNDLE_VERSION, instruments=[row['instrument'] for row in policies],
        sources=[dict(source_id='public.synthetic.calendar', source_kind='repository_snapshot',
                      locator='synthetic.py#'+calendar.bundle_hash, published_on=synthetic.DAYS[0],
                      claim_ceiling='research_observation', contract_version='minute-rule-source-v1'),
                 dict(source_id='public.synthetic.futures', source_kind='repository_evidence', locator='synthetic.py',
                      published_on=synthetic.DAYS[0], claim_ceiling='research_observation', contract_version='minute-rule-source-v1')], rules=[])

    def add(code, kind, parameters, first=synthetic.RUN_DAYS[0], last=synthetic.RUN_DAYS[-1], at=visible, revision=1):
        rules['rules'].append(dict(rule_id='rule.cn_futures.'+kind+'.v1', revision=revision,
            instrument_id=code, asset_class='cn_future', status='supported', effective_from=first, effective_to=last,
            available_at=at, source_ids=['public.synthetic.futures'], parameters=parameters,
            unsupported_reason=None, contract_version='minute-rule-snapshot-v1'))

    for j, code in enumerate(synthetic.CODES):
        expiry = '2024-01-19' if j == 0 else '2024-03-15'
        deadline = '2024-01-18T15:00:00+08:00' if j == 0 else '2024-03-14T15:00:00+08:00'
        add(code, 'contract_lifecycle', dict(product_code='IF', listed_date='2023-01-01', last_trade_date=expiry, exit_deadline=deadline))
        add(code, 'contract_multiplier', dict(contract_multiplier=10, quote_unit='CNY/point'))
        add(code, 'price_tick', dict(price_scale=0, price_tick_units=1))
        add(code, 'session', dict(session_policy_id='session.public.synthetic.future.v1', session_policy_revision=1))
        add(code, 'margin', dict(long_margin_ppm=100000, short_margin_ppm=100000))
        add(code, 'fee_schedule', dict(open_fee_per_contract_units=100, close_fee_per_contract_units=100,
                                      close_today_fee_per_contract_units=100))
        add(code, 'price_limit', dict(price_scale=0, low_limit_units=50, high_limit_units=300,
                                     reference_price_available_at=visible))
        add(code, 'delivery_expiry', dict(close_priority='yesterday_first', exit_deadline=deadline))
        add(code, 'actual_contract_mapping', dict(actual_contract_id=code, mapping_kind='actual_identity_only'))
        for index, day in enumerate(synthetic.RUN_DAYS):
            next_day = synthetic.DAYS[synthetic.DAYS.index(day)+1]
            add(code, 'settlement', dict(price_scale=0, settlement_price_units=108+index*10+j*100,
                settlement_availability_semantics='next_session_open_after_1700'),
                first=day, last=day, at=next_day+'T09:30:00+08:00', revision=index+1)
    rules['rules'].sort(key=lambda row: (row['instrument_id'], row['rule_id'], row['revision']))
    return write_minute_scope_input(package, name='market', calendar=calendar, rules=seal(rules, MinuteRuleSnapshotBundle))
